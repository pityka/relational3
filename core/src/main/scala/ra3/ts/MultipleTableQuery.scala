package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import ra3.lang.ReturnValue
import ra3.lang.ColumnName
import ra3.lang.UnnamedColumn

/** @param input
  * @param predicate
  * @param outputPath
  * @param takes table unique id -> take index array from join , if None then take all
  */
case class MultipleTableQuery(
    input: Seq[SegmentWithName],
    predicate: ra3.lang.Expr,
    outputPath: LogicalPath,
    takes: Seq[(String, Option[SegmentInt])]
)
object MultipleTableQuery {

  def doit(
      input: Seq[SegmentWithName],
      predicate: ra3.lang.Expr,
      outputPath: LogicalPath,
      takes: Seq[(String, Option[SegmentInt])]
  )(implicit tsc: TaskSystemComponents): IO[List[(Segment, String)]] = {
    assert(input.forall(s => takes.exists(_._1 == s.tableUniqueId)))
    val neededColumns = predicate.columnKeys
    val numElems = {
      val d = takes
        .map(v =>
          v._2
            .map(_.numElems)
            .getOrElse(
              input
                .find(_.tableUniqueId == v._1)
                .get
                .segment
                .map(_.numElems)
                .sum
            )
        )
        .distinct
      assert(d.size == 1, "uneven column lengths")
      d.head
    }
    val bIn: IO[List[(ra3.lang.ColumnKey, Buffer, String)]] =
      IO.parSequenceN(32)(input.toList.map {
        case SegmentWithName(segmentParts, tableId, columnName, columnIdx) =>
          val ck = ra3.lang.ColumnKey(tableId, columnIdx)
          if (neededColumns.contains(ck))
            SimpleQuery
              .bufferMultiple(segmentParts)
              .map(b => Some((ck, b, columnName)))
          else IO.pure(None)
      }).map(_.collect { case Some(x) => x })
    val takeBuffers = IO.parSequenceN(32)(takes.map { case (tableId, Some(segment)) =>
      segment.buffer.map { b => (tableId, Some(b)) }
      case (tableId,None) => IO.pure((tableId,None))
    })
    IO.both(bIn, takeBuffers).flatMap { case (buffers, takeBuffers) =>
      val takenBuffers = buffers.map { case (key, buf, x) =>
        val t = takeBuffers.find(_._1 == key.tableUniqueId).get._2
        (key, t.map(t =>buf.take(t)).getOrElse((buf)), x)
      }
      val env1: Map[ra3.lang.Key, ra3.lang.Value[_]] =
        takenBuffers.map { case (key, buf, _) =>
          (key, ra3.lang.Value.Const(buf))
        }.toMap
      val env = env1

      val returnValue =
        ra3.lang.evaluate(predicate, env).v.asInstanceOf[ReturnValue]

      val mask = returnValue.filter

      // If all items are dropped then we do not buffer segments from Star
      // Segments needed for the predicate/projection program are already buffered though
      val maskIsEmpty = mask.exists(_ match {
        case BufferIntConstant(0, _) => true
        case _                       => false
      })

      val selected: IO[List[(Any, String)]] = IO
        .parSequenceN(32)(returnValue.projections.zipWithIndex.map {
          case (ColumnName(value, name), _) => IO.pure(List((value, name)))
          case (UnnamedColumn(value), idx)  => IO.pure(List((value, s"V$idx")))
          case (ra3.lang.Star, _)           =>
            // get those columns who are not yet buffered
            // buffer them and 'take' them
            val r: IO[Seq[(Buffer, String)]] = IO.parSequenceN(32)(
              input
                .map { s =>
                  (
                    s,
                    takenBuffers.find(
                      _._1 == ra3.lang.ColumnKey(s.tableUniqueId, s.columnIdx)
                    )
                  )
                }
                .map {
                  case (_, Some((_, buffer, columnName))) =>
                    IO.pure((buffer, columnName))
                  case (
                        SegmentWithName(segmentParts, tableId, columnName, _),
                        None
                      ) =>
                    if (maskIsEmpty)
                      IO.pure(
                        (segmentParts.head.tag.makeBufferFromSeq(), columnName)
                      )
                    else
                      SimpleQuery
                        .bufferMultiple(segmentParts)
                        .map(b =>
                          (
                            takeBuffers.find(_._1 == tableId).get._2.map(b.take).getOrElse(b),
                            columnName
                          )
                        )
                }
            )
            r
        })
        .map(_.flatten)

      selected.flatMap { selected =>
        val outputNumElemsBeforeFiltering =
          if (maskIsEmpty) 0 else numElems
        IO.parSequenceN(32)(selected.toList.zipWithIndex.map {
          case ((value, columnName), columnIdx) =>
            val buffer = value match {
              case x: Buffer if x.length == outputNumElemsBeforeFiltering => x
              case x: Buffer if outputNumElemsBeforeFiltering == 0 =>
                x.tag.makeBufferFromSeq()
              case x: Buffer =>
                require(
                  false,
                  s"program returned a buffer of size ${x.length} instead of ${outputNumElemsBeforeFiltering}. In this invocation the program must be element wise"
                )
                ???
              case x: Int => BufferIntConstant(x, outputNumElemsBeforeFiltering)
              case x: Double =>
                BufferDouble.constant(x, outputNumElemsBeforeFiltering)
              case x: Long =>
                BufferLong.constant(x, outputNumElemsBeforeFiltering)
              case x: String =>
                BufferString.constant(x, outputNumElemsBeforeFiltering)
              case x => throw new RuntimeException(s"Unknown value returned $x")
            }
            val filtered =
              if (maskIsEmpty) buffer.tag.makeBufferFromSeq()
              else
                mask
                  .map(m => buffer.filter(m))
                  .getOrElse(buffer)

            filtered
              .toSegment(outputPath.copy(column = columnIdx))
              .map(s => (s, columnName))
        })
      }

    }
  }
  def queue(
      input: Seq[SegmentWithName],
      predicate: ra3.lang.Expr { type T <: ReturnValue },
      outputPath: LogicalPath,
      takes: Seq[(String, Option[SegmentInt])]
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Seq[(Segment, String)]] =
    task(MultipleTableQuery(input, predicate.replaceTags(), outputPath, takes))(
      ResourceRequest(
        cpu = (1, 1),
        memory =
          input.flatMap(_.segment).map(ra3.Utils.guessMemoryUsageInMB).sum,
        scratch = 0,
        gpu = 0
      )
    )
  implicit val codec: JsonValueCodec[MultipleTableQuery] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Seq[(Segment, String)]] =
    JsonCodecMaker.make
  val task =
    Task[MultipleTableQuery, Seq[(Segment, String)]]("MultipleTableQuery", 1) {
      case input =>
        implicit ce =>
          doit(input.input, input.predicate, input.outputPath, input.takes)

    }
}

package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import ra3.lang.ReturnValue
import ra3.lang._

/** @param input
  * @param predicate
  * @param outputPath
  * @param takes
  *   table unique id -> take index array from join , if None then take all
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
    scribe.debug(
      s"MultipleTableQuery task on ${input
          .groupBy(_.tableUniqueId)
          .toSeq
          .map(s => (s._1, s._2.map(v => (v.columnName, v.segment.size))))} with $predicate to $outputPath. Takes: ${takes}"
    )

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

    val takeBuffers = IO.parSequenceN(32)(takes.map {
      case (tableId, Some(segment)) =>
        segment.buffer.map { b => (tableId, Some(b)) }
      case (tableId, None) => IO.pure((tableId, None))
    })
    takeBuffers.flatMap { case takeBuffers =>
      val takenBuffers = IO.parSequenceN(32)(
        input
          .map { segmentWithName =>
            val columnKey = ra3.lang.ColumnKey(
              segmentWithName.tableUniqueId,
              segmentWithName.columnIdx
            )
            val t =
              takeBuffers.find(_._1 == segmentWithName.tableUniqueId).get._2
            (columnKey, segmentWithName, t)
            // (key, t.map(t =>buf.take(t)).getOrElse((buf)), x)
          }
          .filter(v => neededColumns.contains(v._1))
          .map {
            case (key, segment, Some(takeBuffer)) if takeBuffer.length == 0 =>
              IO.pure(
                (
                  key,
                  Left(segment.segment.head.tag.makeBufferFromSeq()),
                  segment.columnName
                )
              )
            case (key, segment, Some(takeBuffer)) =>
              ra3.ts.SimpleQuery
                .bufferMultiple(segment.segment)
                .map(b => (key, Left(b.take(takeBuffer)), segment.columnName))
            case (key, segment, None) =>
              ra3.ts.SimpleQuery
                .bufferMultiple(segment.segment)
                .map(b => (key, Left(b), segment.columnName))
          }
      )

      takenBuffers.flatMap { takenBuffers =>
        val env1: Map[ra3.lang.Key, ra3.lang.Value[_]] =
          takenBuffers.map { case (key, bufAsLeft, _) =>
            (key, ra3.lang.Value.Const(bufAsLeft))
          }.toMap
        val env = env1

        ra3.lang
          .evaluate(predicate, env)
          .map(_.v.asInstanceOf[ReturnValue])
          .flatMap { returnValue =>
            val mask = returnValue.filter

            // If all items are dropped then we do not buffer segments from Star
            // Segments needed for the predicate/projection program are already buffered though
            val maskIsEmpty = mask.exists(_ match {
              case Left(BufferIntConstant(0, _))         => true
              case Right(s) if s.forall(_.isConstant(0)) => true
              case _                                     => false
            })

            val selected: IO[List[NamedColumnSpec[_]]] = IO
              .parSequenceN(32)(returnValue.projections.zipWithIndex.map {
                case (v: NamedColumnSpec[_], _) =>
                  IO.pure(List(v))
                case (v: UnnamedColumnSpec[_], idx) =>
                  IO.pure(List(v.withName(s"V$idx")))
                case (ra3.lang.Star, _) =>
                  // get those columns who are not yet buffered
                  // buffer them and 'take' them
                  val r: IO[Seq[ra3.lang.NamedColumnChunk]] =
                    IO.parSequenceN(32)(
                      input
                        .map { s =>
                          (
                            s,
                            takenBuffers.find(
                              _._1 == ra3.lang
                                .ColumnKey(s.tableUniqueId, s.columnIdx)
                            )
                          )
                        }
                        .map {
                          case (_, Some((_, Left(buffer), columnName))) =>
                            IO.pure(
                              ra3.lang
                                .NamedColumnChunk(Left(buffer), columnName)
                            )
                          case (
                                SegmentWithName(
                                  segmentParts,
                                  tableId,
                                  columnName,
                                  _
                                ),
                                _
                              ) =>
                            if (maskIsEmpty)
                              IO.pure(
                                NamedColumnChunk(
                                  Left(
                                    segmentParts.head.tag.makeBufferFromSeq()
                                  ),
                                  columnName
                                )
                              )
                            else
                              ra3.ts.SimpleQuery
                                .bufferMultiple(segmentParts)
                                .map(b =>
                                  NamedColumnChunk(
                                    Left(
                                      takeBuffers
                                        .find(_._1 == tableId)
                                        .get
                                        ._2
                                        .map(b.take)
                                        .getOrElse(b)
                                    ),
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
                case (columnSpec, columnIdx) =>
                  val columnName = columnSpec.name
                  val buffer = columnSpec match {
                    case NamedColumnChunk(Right(_), _) =>
                      throw new AssertionError(
                        "unexpected Right[Seq[Segment]] returned from program"
                      )
                    case NamedColumnChunk(Left(x), _)
                        if x.length == outputNumElemsBeforeFiltering =>
                      x
                    case NamedColumnChunk(Left(x), _)
                        if outputNumElemsBeforeFiltering == 0 =>
                      x.tag.makeBufferFromSeq()
                    case NamedColumnChunk(Left(x), _) =>
                      require(
                        false,
                        s"program returned a buffer of size ${x.length} instead of ${outputNumElemsBeforeFiltering}. In this invocation the program must be element wise"
                      )
                      ???
                    case NamedConstantI32(x, _) =>
                      BufferIntConstant(x, outputNumElemsBeforeFiltering)
                    case NamedConstantF64(x, _) =>
                      BufferDouble.constant(x, outputNumElemsBeforeFiltering)
                    case NamedConstantI64(x, _) =>
                      BufferLong.constant(x, outputNumElemsBeforeFiltering)
                    case NamedConstantString(x, _) =>
                      BufferString.constant(x, outputNumElemsBeforeFiltering)

                  }
                  val filtered =
                    if (maskIsEmpty) IO.pure(buffer.tag.makeBufferFromSeq())
                    else {
                      mask match {
                        case None => IO.pure(buffer)
                        case Some(mask) =>
                          ra3.lang.bufferIfNeeded(mask).map(buffer.filter)
                      }
                    }

                  filtered.flatMap(
                    _.toSegment(outputPath.copy(column = columnIdx))
                      .map(s => (s, columnName))
                  )
              })
            }
          }
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
    task(
      MultipleTableQuery(
        input,
        predicate.replaceTags(Map.empty),
        outputPath,
        takes
      )
    )(
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

package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO
import ra3.lang.ReturnValueTuple
import ra3.lang.*

/** @param input
  * @param predicate
  * @param outputPath
  * @param takes
  *   table unique id -> take index array from join , if None then take all
  */
private[ra3] case class MultipleTableQuery(
    input: Seq[(ColumnTag, SegmentWithName)],
    predicate: ra3.lang.runtime.Expr,
    outputPath: LogicalPath,
    takes: Seq[(String, Option[SegmentInt])]
)
private[ra3] object MultipleTableQuery {

  private def doit(
      input: Seq[(ColumnTag, SegmentWithName)],
      predicate: ra3.lang.runtime.Expr,
      outputPath: LogicalPath,
      takes: Seq[(String, Option[SegmentInt])]
  )(implicit tsc: TaskSystemComponents): IO[List[(TaggedSegment, String)]] = {
    scribe.debug(
      s"MultipleTableQuery task on ${input
          .groupBy(_._2.tableUniqueId)
          .toSeq
          .map(s => (s._1, s._2.map(v => (v._2.columnName, v._2.segment.size))))} with $predicate to $outputPath. Takes: ${takes}"
    )

    assert(input.forall(s => takes.exists(_._1 == s._2.tableUniqueId)))
    val neededColumns = predicate.columnKeys
    val numElems = {
      val d = takes
        .map(v =>
          v._2
            .map(_.numElems)
            .getOrElse {
              input
                .find(_._2.tableUniqueId == v._1)
                .get
                ._2
                .segment
                .map(_.numElems)
                .sum
            }
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
              segmentWithName._2.tableUniqueId,
              segmentWithName._2.columnIdx
            )
            val t =
              takeBuffers.find(_._1 == segmentWithName._2.tableUniqueId).get._2
            (columnKey, segmentWithName, t)
          }
          .filter(v => neededColumns.contains(v._1))
          .map {
            case (key, segment, Some(takeBuffer)) if takeBuffer.length == 0 =>
              val tag = segment._1
              IO.pure(
                (
                  key,
                  (tag.makeTaggedBuffer(tag.makeBufferFromSeq())),
                  segment._2.columnName
                )
              )
            case (key, (tag, segment), Some(takeBuffer)) =>
              ra3.Utils
                .bufferMultiple(tag)(
                  segment.segment.map(_.asInstanceOf[tag.SegmentType])
                )
                .map(b =>
                  (
                    key,
                    (tag.makeTaggedBuffer(tag.take(b, takeBuffer))),
                    segment.columnName
                  )
                )
            case (key, (tag, segment), None) =>
              ra3.Utils
                .bufferMultiple(tag)(
                  segment.segment.map(_.asInstanceOf[tag.SegmentType])
                )
                .map(b => (key, (tag.makeTaggedBuffer(b)), segment.columnName))
          }
      )

      takenBuffers.flatMap { takenBuffers =>
        val env1: Map[ra3.lang.Key, ra3.lang.runtime.Value] =
          takenBuffers.map { case (key, bufAsLeft, _) =>
            (key, ra3.lang.runtime.Value(bufAsLeft.wrap))
          }.toMap
        val env = env1

        ra3.lang.runtime.Expr
          .evaluate(predicate, env)
          .map(_.v.asInstanceOf[ReturnValueTuple[?, ?]])
          .flatMap { returnValue =>
            val mask = returnValue.filter

            // If all items are dropped then we do not buffer segments from Star
            // Segments needed for the predicate/projection program are already buffered though
            val maskIsEmpty = mask.exists(_ match {
              case Left(BufferIntConstant(0, _))         => true
              case Right(s) if s.forall(_.isConstant(0)) => true
              case _                                     => false
            })

            val selected: IO[List[ColumnSpec[?, ?]]] = IO
              .parSequenceN(32)(
                ReturnValueTuple.list(returnValue).zipWithIndex.map {
                  case (v: ColumnSpec[?, ?], _) =>
                    IO.pure((v))
                  // case x =>
                  //   throw new RuntimeException("Unexpected unmatched case "+x)
                }
              )

            selected.flatMap { selected =>
              val outputNumElemsBeforeFiltering =
                if (maskIsEmpty) 0 else numElems
              IO.parSequenceN(32)(selected.toList.zipWithIndex.map {
                case (columnSpec, columnIdx) =>
                  val columnName = columnSpec.name

                  val taggedBuffer = columnSpec match {
                    case NamedColumnSpecWithColumnChunkValueExtractor(
                          Right(_),
                          _
                        ) =>
                      throw new AssertionError(
                        "unexpected Right[Seq[Segment]] returned from program"
                      )
                    case NamedColumnSpecWithColumnChunkValueExtractor(
                          Left(x),
                          _
                        ) if x.buffer.length == outputNumElemsBeforeFiltering =>
                      x.tag.makeTaggedBuffer(x.buffer)
                    case NamedColumnSpecWithColumnChunkValueExtractor(
                          Left(x),
                          _
                        ) if outputNumElemsBeforeFiltering == 0 =>
                      x.tag.makeTaggedBuffer(x.tag.makeBufferFromSeq())
                    case NamedColumnSpecWithColumnChunkValueExtractor(
                          Left(x),
                          _
                        ) =>
                      require(
                        false,
                        s"program returned a buffer of size ${x.buffer.length} instead of ${outputNumElemsBeforeFiltering}. In this invocation the program must be element wise"
                      )
                      ???
                    case NamedConstantI32(x, _) =>
                      ColumnTag.I32.makeTaggedBuffer(
                        BufferIntConstant(x, outputNumElemsBeforeFiltering)
                      )
                    case NamedConstantF64(x, _) =>
                      ColumnTag.F64.makeTaggedBuffer(
                        BufferDouble.constant(x, outputNumElemsBeforeFiltering)
                      )
                    case NamedConstantI64(x, _) =>
                      ColumnTag.I64.makeTaggedBuffer(
                        BufferLong.constant(x, outputNumElemsBeforeFiltering)
                      )
                    case NamedConstantString(x, _) =>
                      ColumnTag.StringTag.makeTaggedBuffer(
                        BufferString.constant(x, outputNumElemsBeforeFiltering)
                      )
                    case NamedConstantInstant(x, _) =>
                      ColumnTag.Instant.makeTaggedBuffer(
                        BufferInstant.constant(
                          x.toEpochMilli(),
                          outputNumElemsBeforeFiltering
                        )
                      )
                    case x =>
                      throw new RuntimeException(
                        "Unexpected unmatched case " + x
                      )
                  }
                  val tag = taggedBuffer.tag
                  val buffer = taggedBuffer.buffer
                  val filtered =
                    if (maskIsEmpty) IO.pure(tag.makeBufferFromSeq())
                    else {
                      mask match {
                        case None => IO.pure(buffer)
                        case Some(mask) =>
                          ra3.lang.util
                            .bufferIfNeeded(ColumnTag.I32)(mask)
                            .map(tag.filter(buffer, _))
                      }
                    }

                  filtered.flatMap(
                    tag
                      .toSegment(_, outputPath.copy(column = columnIdx))
                      .map(s => (tag.makeTaggedSegment(s), columnName))
                  )
              })
            }
          }
      }
    }.logElapsed
  }
  def queue(
      input: Seq[TypedSegmentWithName],
      predicate: ra3.lang.runtime.Expr,
      outputPath: LogicalPath,
      takes: Seq[(String, Option[SegmentInt])]
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Seq[(TaggedSegment, String)]] =
    IO {
      scribe.debug(
        s"Queueing MultipleTableQuery on ${input.size} table segments"
      )
    } *> task(
      MultipleTableQuery(
        input.map(v => v.tag -> v.erase),
        predicate,
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
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[MultipleTableQuery] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Seq[(TaggedSegment, String)]] =
    JsonCodecMaker.make
  // $COVERAGE-ON$
  val task =
    Task[MultipleTableQuery, Seq[(TaggedSegment, String)]](
      "MultipleTableQuery",
      1
    ) { case input =>
      implicit ce =>
        doit(input.input, input.predicate, input.outputPath, input.takes)

    }
}

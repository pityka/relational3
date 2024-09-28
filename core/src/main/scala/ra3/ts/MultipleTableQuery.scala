package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO
import ra3.lang.ReturnValue
import ra3.lang.*

/** @param input
  * @param predicate
  * @param outputPath
  * @param takes
  *   table unique id -> take index array from join , if None then take all
  */
private[ra3] case class MultipleTableQuery(
    input: Seq[(ColumnTag, SegmentWithName)],
    predicate: ra3.lang.Expr[?],
    outputPath: LogicalPath,
    takes: Seq[(String, Option[SegmentInt])]
)
private[ra3] object MultipleTableQuery {

  def doit(
      input: Seq[(ColumnTag, SegmentWithName)],
      predicate: ra3.lang.Expr[ReturnValue[?]],
      outputPath: LogicalPath,
      takes: Seq[(String, Option[SegmentInt])]
  )(implicit tsc: TaskSystemComponents): IO[List[(Segment, String)]] = {
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
                  Left(tag.makeTaggedBuffer(tag.makeBufferFromSeq())),
                  segment._2.columnName
                )
              )
            case (key, (tag,segment), Some(takeBuffer)) =>
              ra3.Utils
                .bufferMultiple(tag)(segment.segment.map(_.asInstanceOf[tag.SegmentType]))
                .map(b => (key, Left(tag.makeTaggedBuffer(tag.take(b,takeBuffer))), segment.columnName))
            case (key, (tag,segment), None) =>
              ra3.Utils
                .bufferMultiple(tag)(segment.segment.map(_.asInstanceOf[tag.SegmentType]))
                .map(b => (key, Left(tag.makeTaggedBuffer(b)), segment.columnName))
          }
      )

      takenBuffers.flatMap { takenBuffers =>
        val env1: Map[ra3.lang.Key, ra3.lang.Value[?]] =
          takenBuffers.map { case (key, bufAsLeft, _) =>
            (key, ra3.lang.Value.Const(bufAsLeft))
          }.toMap
        val env = env1

        ra3.lang
          .evaluate(predicate, env)
          .map(_.v)
          .flatMap { returnValue =>
            val mask = returnValue.filter

            // If all items are dropped then we do not buffer segments from Star
            // Segments needed for the predicate/projection program are already buffered though
            val maskIsEmpty = mask.exists(_ match {
              case Left(BufferIntConstant(0, _))         => true
              case Right(s) if s.forall(_.isConstant(0)) => true
              case _                                     => false
            })

            val selected: IO[List[NamedColumnSpec[?]]] = IO
              .parSequenceN(32)(ReturnValue.list(returnValue).zipWithIndex.map {
                case (v: NamedColumnSpec[?], _) =>
                  IO.pure(List(v))
                case (v: UnnamedColumnSpec[?], idx) =>
                  IO.pure(List(v.withName(s"V$idx")))
                case (ra3.lang.StarColumnSpec, _) =>
                  // get those columns who are not yet buffered
                  // buffer them and 'take' them
                  val r =
                    IO.parSequenceN(32)(
                      input
                        .map { s =>
                          (
                            s,
                            takenBuffers.find(
                              _._1 == ra3.lang
                                .ColumnKey(s._2.tableUniqueId, s._2.columnIdx)
                            )
                          )
                        }
                        .map {
                          case ((tag,_), Some((_, Left(buffer), columnName))) =>
                            IO.pure(
                              tag.makeNamedColumnSpecFromBuffer(buffer.asInstanceOf[tag.BufferType],columnName)
                            )
                          case (
                                (tag,SegmentWithName(
                                  segmentParts,
                                  tableId,
                                  columnName,
                                  _
                                )),
                                _
                              ) =>
                            if (maskIsEmpty)
                              IO.pure(
                                tag.makeNamedColumnSpecFromBuffer(tag.makeBufferFromSeq(),columnName)
                              )
                            else
                              ra3.Utils
                                .bufferMultiple(tag)(segmentParts.map(_.asInstanceOf[tag.SegmentType]))
                                .map(b =>
                                  tag.makeNamedColumnSpecFromBuffer(
                                      takeBuffers
                                        .find(_._1 == tableId)
                                        .get
                                        ._2
                                        .map(tag.take(b,_))
                                        .getOrElse(b)
                                    ,
                                    columnName
                                  )
                                )
                        }
                    )
                  r
                case x => 
                  throw new RuntimeException("Unexpected unmatched case "+x)
              })
              .map(_.flatten)

            selected.flatMap { selected =>
              val outputNumElemsBeforeFiltering =
                if (maskIsEmpty) 0 else numElems
              IO.parSequenceN(32)(selected.toList.zipWithIndex.map {
                case (columnSpec, columnIdx) =>
                  val columnName = columnSpec.name

                  val taggedBuffer = columnSpec match {
                    case NamedColumnSpecWithColumnChunkValueExtractor(Right(_), _) =>
                      throw new AssertionError(
                        "unexpected Right[Seq[Segment]] returned from program"
                      )
                    case NamedColumnSpecWithColumnChunkValueExtractor(Left(x), _)
                        if x.buffer.length == outputNumElemsBeforeFiltering =>
                      x.tag.makeTaggedBuffer(x.buffer)
                    case NamedColumnSpecWithColumnChunkValueExtractor(Left(x), _)
                        if outputNumElemsBeforeFiltering == 0 =>
                      x.tag.makeTaggedBuffer(x.tag.makeBufferFromSeq())
                    case NamedColumnSpecWithColumnChunkValueExtractor(Left(x), _) =>
                      require(
                        false,
                        s"program returned a buffer of size ${x.buffer.length} instead of ${outputNumElemsBeforeFiltering}. In this invocation the program must be element wise"
                      )
                      ???
                    case NamedConstantI32(x, _) =>
                      ColumnTag.I32.makeTaggedBuffer(BufferIntConstant(x, outputNumElemsBeforeFiltering))
                    case NamedConstantF64(x, _) =>
                      ColumnTag.F64.makeTaggedBuffer(BufferDouble.constant(x, outputNumElemsBeforeFiltering))
                    case NamedConstantI64(x, _) =>
                      ColumnTag.I64.makeTaggedBuffer(BufferLong.constant(x, outputNumElemsBeforeFiltering))
                    case NamedConstantString(x, _) =>
                      ColumnTag.StringTag.makeTaggedBuffer(BufferString.constant(x, outputNumElemsBeforeFiltering))
                    case x => 
                      throw new RuntimeException("Unexpected unmatched case "+x)
                  }
                  val tag = taggedBuffer.tag
                  val buffer = taggedBuffer.buffer
                  val filtered =
                    if (maskIsEmpty) IO.pure(tag.makeBufferFromSeq())
                    else {
                      mask match {
                        case None => IO.pure(buffer)
                        case Some(mask) =>
                          ra3.lang.bufferIfNeeded(ColumnTag.I32)(mask).map(tag.filter(buffer,_))
                      }
                    }

                  filtered.flatMap(
                    tag.toSegment(_,outputPath.copy(column = columnIdx))
                      .map(s => (s, columnName))
                  )
              })
            }
          }
      }
    }
  }
  def queue(
      input: Seq[TypedSegmentWithName],
      predicate: ra3.lang.Expr[ReturnValue[?]],
      outputPath: LogicalPath,
      takes: Seq[(String, Option[SegmentInt])]
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Seq[(ColumnTag, Segment, String)]] =
    task(
      MultipleTableQuery(
        input.map(v => v.tag -> v.erase),
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
    ).map(seq => seq.zip(input.map(_.tag)).map(v => (v._2, v._1._1, v._1._2)))
  implicit val codec: JsonValueCodec[MultipleTableQuery] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Seq[(Segment, String)]] =
    JsonCodecMaker.make
  val task =
    Task[MultipleTableQuery, Seq[(Segment, String)]]("MultipleTableQuery", 1) {
      case input =>
        implicit ce =>
          doit(input.input, input.predicate.asInstanceOf[Expr[ReturnValue[?]]], input.outputPath, input.takes)

    }
}

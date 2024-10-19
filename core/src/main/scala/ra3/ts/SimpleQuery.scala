package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO
import ra3.lang.*

private[ra3] class TypedSegmentWithName(
    val tag: ColumnTag,
    val segment: Seq[
      tag.SegmentType
    ], // buffer and cat all of them, treat as one group
    tableUniqueId: String,
    columnName: String,
    columnIdx: Int
) {
  def erase = SegmentWithName(segment, tableUniqueId, columnName, columnIdx)
  override def toString =
    s"TypedSegmentWithName(table=$tableUniqueId,columnName=$columnName,columnIdx=$columnIdx,segments=${segment
        .map(s => (tag, s.numElems))})"
}
private[ra3] case class SegmentWithName(
    segment: Seq[
      Segment
    ], // buffer and cat all of them, treat as one group
    tableUniqueId: String,
    columnName: String,
    columnIdx: Int
) {
  override def toString =
    s"SegmentWithName(table=$tableUniqueId,columnName=$columnName,columnIdx=$columnIdx,segments=${segment
        .map(s => (s.numElems))})"
}

private[ra3] case class SimpleQuery(
    input: Seq[(ColumnTag, SegmentWithName)],
    predicate: ra3.lang.runtime.Expr,
    outputPath: LogicalPath,
    groupMap: Option[(SegmentInt, Int)]
)
private[ra3] object SimpleQuery {

  private def doit(
      input: Seq[(ColumnTag, SegmentWithName)],
      predicate: ra3.lang.runtime.Expr,
      outputPath: LogicalPath,
      groupMap: Option[(SegmentInt, Int)]
  )(implicit tsc: TaskSystemComponents): IO[List[(TaggedSegment, String)]] = {
    scribe.debug(
      s"SimpleQuery task on ${input
          .groupBy(_._2.tableUniqueId)
          .toSeq
          .map(s => (s._1, s._2.map(v => (v._2.columnName, v._2.segment.size))))} with $predicate to $outputPath. Grouping: $groupMap"
    )
    val neededColumns = predicate.columnKeys
    val numElems = {
      val d = input.map(_._2.segment.map(_.numElems).sum).distinct
      assert(d.size == 1, "uneven column lengths")
      d.head
    }

    val groupMapBuffer = groupMap match {
      case None             => IO.pure(None)
      case Some((map, num)) => map.buffer.map(s => Some((s, num)))
    }
    // IO.both(bIn, groupMapBuffer)
    groupMapBuffer.flatMap { case groupMapBuffer =>
      val env1: Map[ra3.lang.Key, ra3.lang.runtime.Value] =
        input
          .map { case (tag, segmentWithName) =>
            val columnKey = ra3.lang.ColumnKey(
              segmentWithName.tableUniqueId,
              segmentWithName.columnIdx
            )
            (columnKey, ra3.lang.runtime.Value(Right(segmentWithName.segment)))
          }
          .filter(v => neededColumns.contains(v._1))
          .toMap
      val env = env1 ++ groupMapBuffer.toList.flatMap { case (map, num) =>
        Seq(
          ra3.lang.GroupMap -> ra3.lang.runtime.Value(map),
          ra3.lang.Numgroups -> ra3.lang.runtime.Value(num)
        )
      }

      ra3.lang.runtime.Expr
        .evaluate(predicate, env)
        .map(_.v.asInstanceOf[ReturnValueTuple[?]])
        .flatMap { returnValue =>
          val mask = returnValue.filter

          // If all items are dropped then we do not buffer segments from Star
          // Segments needed for the predicate/projection program are already buffered though
          val maskIsEmpty = mask.exists(_ match {
            case Left(BufferIntConstant(0, _))         => true
            case Right(s) if s.forall(_.isConstant(0)) => true
            case _                                     => false
          })

          val maskIsComplete = mask match {
            case None                                        => true
            case Some(Left(BufferIntConstant(1, _)))         => true
            case Some(Right(s)) if s.forall(_.isConstant(1)) => true
            case _                                           => false
          }

          scribe.debug(
            s"SQ program evaluation done projection: ${ReturnValueTuple
                .list(returnValue)} filter: ${returnValue.filter} maskIsEmpty=$maskIsEmpty maskIsComplete=$maskIsComplete"
          )

          val selected: IO[List[NamedColumnSpec[?]]] = IO
            .parSequenceN(32)(
              ReturnValueTuple.list(returnValue).zipWithIndex.map {
                case (v: NamedColumnSpec[?], _) =>
                  IO.pure((v))
                case (v: UnnamedColumnSpec[?], idx) =>
                  IO.pure((v.withName(s"V$idx")))
                // case x =>
                //   throw new RuntimeException("Unexpected unmatched case "+x)
              }
            )

          val fusedSegments: IO[List[NamedColumnSpec[?]]] = selected.flatMap {
            list =>
              IO.parSequenceN(32)(list.map { case value =>
                value match {
                  case NamedColumnSpecWithColumnChunkValueExtractor(
                        Right(x),
                        name
                      ) if x.segments.size > 1 =>
                    Utils
                      .bufferMultiple(x.tag)(x.segments)
                      .map(b => x.tag.makeNamedColumnSpecFromBuffer(b, name))
                  case x => IO.pure(x)
                }
              })
          }

          fusedSegments.flatMap { selected =>
            val outputNumElems =
              if (maskIsEmpty) 0 else groupMap.map(_._2).getOrElse(numElems)
            IO.parSequenceN(32)(selected.toList.zipWithIndex.map {
              case (columnSpec, columnIdx) =>
                val columnName = columnSpec.name
                val bufferOrSegment: Either[TaggedBuffer, TaggedSegment] =
                  columnSpec match {
                    case NamedColumnSpecWithColumnChunkValueExtractor(
                          Left(x),
                          _
                        ) if x.buffer.length == outputNumElems =>
                      Left(x)
                    case NamedColumnSpecWithColumnChunkValueExtractor(
                          Left(x),
                          _
                        ) if outputNumElems == 0 =>
                      Left(x.tag.makeTaggedBuffer(x.tag.makeBufferFromSeq()))
                    case NamedColumnSpecWithColumnChunkValueExtractor(
                          Left(_),
                          _
                        ) =>
                      require(
                        false,
                        "in grouped query you must use an aggregator function on the columns. Use .first to take first item per group"
                      )
                      ???
                    // x.take(BufferInt.apply(Array.fill(outputNumElems)(0)))
                    case NamedColumnSpecWithColumnChunkValueExtractor(
                          Right(x),
                          _
                        ) if x.segments.size == 1 =>
                      Right(x.tag.makeTaggedSegment(x.segments.head))
                    case NamedColumnSpecWithColumnChunkValueExtractor(
                          Right(_),
                          _
                        ) =>
                      throw new AssertionError(
                        "Error, unexpected Right[Seq[Segment]] at this point. Should have been handed in fuseSegments"
                      )
                    case NamedConstantI32(x, _) =>
                      Left(
                        ColumnTag.I32
                          .makeTaggedBuffer(
                            BufferIntConstant(x, outputNumElems)
                          )
                      )
                    case NamedConstantF64(x, _) =>
                      Left(
                        ColumnTag.F64.makeTaggedBuffer(
                          BufferDouble.constant(x, outputNumElems)
                        )
                      )
                    case NamedConstantI64(x, _) =>
                      Left(
                        ColumnTag.I64.makeTaggedBuffer(
                          BufferLong.constant(x, outputNumElems)
                        )
                      )
                    case NamedConstantString(x, _) =>
                      Left(
                        ColumnTag.StringTag.makeTaggedBuffer(
                          BufferString.constant(x, outputNumElems)
                        )
                      )

                    case NamedConstantInstant(x, _) =>
                      Left(
                        ColumnTag.Instant.makeTaggedBuffer(
                          BufferInstant
                            .constant(x.toEpochMilli(), outputNumElems)
                        )
                      )
                    case x =>
                      throw new RuntimeException(
                        "Unexpected unmatched case " + x
                      )

                  }
                val filteredSegment: IO[TaggedSegment] =
                  if (maskIsEmpty) {
                    val tag = (bufferOrSegment match {
                      case Left(value)  => value.tag
                      case Right(value) => value.tag
                    })

                    tag
                      .toSegment(
                        tag.makeBufferFromSeq(),
                        outputPath.copy(column = columnIdx)
                      )
                      .map(tag.makeTaggedSegment)
                  } else if (maskIsComplete && bufferOrSegment.isRight) {
                    IO.pure(bufferOrSegment.toOption.get)
                  } else {
                    val maskableBuffer = bufferOrSegment match {
                      case Left(b) => IO.pure(b)
                      case Right(s) =>
                        s.tag.buffer(s.segment).map(s.tag.makeTaggedBuffer)
                    }
                    maskableBuffer.flatMap { maskableBuffer =>
                      (mask match {
                        case None => IO.pure(maskableBuffer)
                        case Some(mask) =>
                          ra3.lang.util
                            .bufferIfNeeded(ColumnTag.I32)(mask)
                            .map(mask =>
                              maskableBuffer.tag.makeTaggedBuffer(
                                maskableBuffer.tag
                                  .filter(maskableBuffer.buffer, mask)
                              )
                            )
                      }).flatMap(tb =>
                        tb.tag
                          .toSegment(
                            tb.buffer,
                            outputPath.copy(column = columnIdx)
                          )
                          .map(tb.tag.makeTaggedSegment)
                      )
                    }

                  }

                filteredSegment
                  .map(s => (s, columnName))
            })
          }
        }

    }
  }
  def queue(
      // (segment, table unique id)
      input: Seq[TypedSegmentWithName],
      predicate: ra3.lang.runtime.Expr,
      outputPath: LogicalPath,
      groupMap: Option[(SegmentInt, Int)]
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Seq[(TaggedSegment, String)]] =
  IO {
      scribe.debug(
        s"Queueing SimpleQuery on ${input.size} table segments. Groups present: ${groupMap.isDefined}"
      )
    } *>task(
    SimpleQuery(
      input.map(v => v.tag -> v.erase),
      predicate,
      outputPath,
      groupMap
    )
  )(
    ResourceRequest(
      cpu = (1, 1),
      memory = input.flatMap(_.segment).map(ra3.Utils.guessMemoryUsageInMB).sum,
      scratch = 0,
      gpu = 0
    )
  )
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[SimpleQuery] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Seq[(TaggedSegment, String)]] =
  JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[SimpleQuery, Seq[(TaggedSegment, String)]]("SimpleQuery", 1) {
    case input =>
      implicit ce =>
        doit(
          input.input,
          input.predicate,
          input.outputPath,
          input.groupMap
        )

  }
}

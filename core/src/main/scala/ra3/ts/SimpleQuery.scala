package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import ra3.lang.ReturnValue
import ra3.lang._

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
        .map(s => (s.tag, s.numElems))})"
}

private[ra3] case class SimpleQuery(
    input: Seq[SegmentWithName],
    predicate: ra3.lang.Expr,
    outputPath: LogicalPath,
    groupMap: Option[(SegmentInt, Int)]
)
private[ra3] object SimpleQuery {

  def bufferMultiple(s: Seq[Segment])(implicit tsc: TaskSystemComponents) = {
    val tag = s.head.tag
    IO
      .parSequenceN(32)(s.map(_.as(tag).buffer.map(_.asBufferType)))
      .map(b => tag.cat(b: _*))
  }

  def doit(
      input: Seq[SegmentWithName],
      predicate: ra3.lang.Expr,
      outputPath: LogicalPath,
      groupMap: Option[(SegmentInt, Int)]
  )(implicit tsc: TaskSystemComponents): IO[List[(Segment, String)]] = {
    scribe.debug(
      s"SimpleQuery task on ${input.groupBy(_.tableUniqueId).toSeq.map(s => (s._1, s._2.map(v => (v.columnName, v.segment.size))))} with $predicate to $outputPath. Grouping: $groupMap"
    )
    val neededColumns = predicate.columnKeys
    val numElems = {
      val d = input.map(_.segment.map(_.numElems).sum).distinct
      assert(d.size == 1, "uneven column lengths")
      d.head
    }

    val groupMapBuffer = groupMap match {
      case None             => IO.pure(None)
      case Some((map, num)) => map.buffer.map(s => Some((s, num)))
    }
    // IO.both(bIn, groupMapBuffer)
    groupMapBuffer.flatMap { case groupMapBuffer =>
      val env1: Map[ra3.lang.Key, ra3.lang.Value[_]] =
        input
          .map { case segmentWithName =>
            val columnKey = ra3.lang.ColumnKey(
              segmentWithName.tableUniqueId,
              segmentWithName.columnIdx
            )
            (columnKey, ra3.lang.Value.Const(Right(segmentWithName.segment)))
          }
          .filter(v => neededColumns.contains(v._1))
          .toMap
      val env = env1 ++ groupMapBuffer.toList.flatMap { case (map, num) =>
        Seq(
          ra3.lang.GroupMap -> ra3.lang.Value
            .Const(map),
          ra3.lang.Numgroups -> ra3.lang.Value.Const(num)
        )
      }

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

          val maskIsComplete = mask match {
            case None                                        => true
            case Some(Left(BufferIntConstant(1, _)))         => true
            case Some(Right(s)) if s.forall(_.isConstant(1)) => true
            case _                                           => false
          }

          scribe.debug(
            s"SQ program evaluation done projection: ${returnValue.projections} filter: ${returnValue.filter} maskIsEmpty=$maskIsEmpty maskIsComplete=$maskIsComplete"
          )

          val selected: IO[List[NamedColumnSpec[_]]] = IO
            .parSequenceN(32)(returnValue.projections.zipWithIndex.map {
              case (v: NamedColumnSpec[_], _) =>
                IO.pure(List(v))
              case (v: UnnamedColumnSpec[_], idx) =>
                IO.pure(List(v.withName(s"V$idx")))
              case (ra3.lang.Star, _) =>
                val r: IO[Seq[NamedColumnChunk]] =
                  IO.parSequenceN(32)(
                    input
                      .map {
                        case SegmentWithName(segmentParts, _, columnName, _) =>
                          if (maskIsEmpty)
                            IO.pure(
                              NamedColumnChunk(
                                Left(segmentParts.head.tag.makeBufferFromSeq()),
                                columnName
                              )
                            )
                          else if (maskIsComplete && segmentParts.size == 1)
                            IO.pure(
                              NamedColumnChunk(Right(segmentParts), columnName)
                            )
                          else
                            bufferMultiple(segmentParts)
                              .map(b => NamedColumnChunk(Left(b), columnName))
                      }
                  )
                r
            })
            .map(_.flatten)

          val fusedSegments: IO[List[NamedColumnSpec[_]]] = selected.flatMap {
            list =>
              IO.parSequenceN(32)(list.map { case value =>
                value match {
                  case NamedColumnChunk(Right(x), name) if x.size > 1 =>
                    bufferMultiple(x).map(b => NamedColumnChunk(Left(b), name))
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
                val bufferOrSegment = columnSpec match {
                  case NamedColumnChunk(Left(x), _)
                      if x.length == outputNumElems =>
                    Left(x)
                  case NamedColumnChunk(Left(x), _) if outputNumElems == 0 =>
                    Left(x.tag.makeBufferFromSeq())
                  case NamedColumnChunk(Left(_), _) =>
                    require(
                      false,
                      "in grouped query you must use an aggregator function on the columns. Use .first to take first item per group"
                    )
                    ???
                  // x.take(BufferInt.apply(Array.fill(outputNumElems)(0)))
                  case NamedColumnChunk(Right(x), _) if x.size == 1 =>
                    Right(x.head)
                  case NamedColumnChunk(Right(_), _) =>
                    throw new AssertionError(
                      "Error, unexpected Right[Seq[Segment]] at this point. Should have been handed in fuseSegments"
                    )
                  case NamedConstantI32(x, _) =>
                    Left(BufferIntConstant(x, outputNumElems))
                  case NamedConstantF64(x, _) =>
                    Left(BufferDouble.constant(x, outputNumElems))
                  case NamedConstantI64(x, _) =>
                    Left(BufferLong.constant(x, outputNumElems))
                  case NamedConstantString(x, _) =>
                    Left(BufferString.constant(x, outputNumElems))

                }
                val filteredSegment =
                  if (maskIsEmpty)
                    (bufferOrSegment match {
                      case Left(value)  => value.tag
                      case Right(value) => value.tag
                    }).makeBufferFromSeq()
                      .toSegment(outputPath.copy(column = columnIdx))
                  else if (maskIsComplete && bufferOrSegment.isRight)
                    IO.pure(bufferOrSegment.toOption.get)
                  else {
                    val maskableBuffer = bufferOrSegment match {
                      case Left(b)  => IO.pure(b)
                      case Right(s) => s.buffer
                    }
                    maskableBuffer.flatMap { maskableBuffer =>
                      (mask match {
                        case None => IO.pure(maskableBuffer)
                        case Some(mask) =>
                          ra3.lang
                            .bufferIfNeeded(mask)
                            .map(maskableBuffer.filter)
                      }).flatMap(
                        _.toSegment(outputPath.copy(column = columnIdx))
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
      input: Seq[SegmentWithName],
      predicate: ra3.lang.Expr { type T <: ReturnValue },
      outputPath: LogicalPath,
      groupMap: Option[(SegmentInt, Int)]
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Seq[(Segment, String)]] =
    task(
      SimpleQuery(input, predicate.replaceTags(Map.empty), outputPath, groupMap)
    )(
      ResourceRequest(
        cpu = (1, 1),
        memory =
          input.flatMap(_.segment).map(ra3.Utils.guessMemoryUsageInMB).sum,
        scratch = 0,
        gpu = 0
      )
    )
  implicit val codec: JsonValueCodec[SimpleQuery] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Seq[(Segment, String)]] =
    JsonCodecMaker.make
  val task = Task[SimpleQuery, Seq[(Segment, String)]]("SimpleQuery", 1) {
    case input =>
      implicit ce =>
        doit(input.input, input.predicate, input.outputPath, input.groupMap)

  }
}

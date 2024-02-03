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

case class SegmentWithName(
    segment: Seq[
      Segment
    ], // buffer and cat all of them, treat as one group
    tableUniqueId: String,
    columnName: String,
    columnIdx: Int
)

case class SimpleQuery(
    input: Seq[SegmentWithName],
    predicate: ra3.lang.Expr,
    outputPath: LogicalPath,
    groupMap: Option[(SegmentInt, Int)]
)
object SimpleQuery {

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
    val neededColumns = predicate.columnKeys
    val numElems = {
      val d = input.map(_.segment.map(_.numElems).sum).distinct
      assert(d.size == 1, "uneven column lengths")
      d.head
    }
    val bIn: IO[List[(ra3.lang.ColumnKey, Buffer, String)]] =
      IO.parSequenceN(32)(input.toList.map {
        case SegmentWithName(segmentParts, tableId, columnName, columnIdx) =>
          val ck = ra3.lang.ColumnKey(tableId, columnIdx)
          if (neededColumns.contains(ck))
            bufferMultiple(segmentParts).map(b => Some((ck, b, columnName)))
          else IO.pure(None)
      }).map(_.collect { case Some(x) => x })
    val groupMapBuffer = groupMap match {
      case None             => IO.pure(None)
      case Some((map, num)) => map.buffer.map(s => Some((s, num)))
    }
    IO.both(bIn, groupMapBuffer).flatMap { case (buffers, groupMapBuffer) =>
      val env1: Map[ra3.lang.Key, ra3.lang.Value[_]] =
        buffers.map { case (key, buf, _) =>
          (key, ra3.lang.Value.Const(buf))
        }.toMap
      val env = env1 ++ groupMapBuffer.toList.flatMap { case (map, num) =>
        Seq(
          ra3.lang.GroupMap -> ra3.lang.Value
            .Const(map),
          ra3.lang.Numgroups -> ra3.lang.Value.Const(num)
        )
      }

      val returnValue =
        ra3.lang.evaluate(predicate, env).v.asInstanceOf[ReturnValue]

      val mask = returnValue.filter

      // If all items are dropped then we do not buffer segments from Star
      // Segments needed for the predicate/projection program are already buffered though
      val maskIsEmpty = mask.exists(_ match {
        case BufferIntConstant(0, _) => true
        case _                       => false
      })

      val maskIsComplete = mask match {
        case None                          => true
        case Some(BufferIntConstant(1, _)) => true
        case _                             => false
      }

      val selected: IO[List[(Any, String)]] = IO
        .parSequenceN(32)(returnValue.projections.zipWithIndex.map {
          case (ColumnName(value, name), _) => IO.pure(List((value, name)))
          case (UnnamedColumn(value), idx)  => IO.pure(List((value, s"V$idx")))
          case (ra3.lang.Star, _) =>
            val r: IO[Seq[(Either[Buffer, Segment], String)]] =
              IO.parSequenceN(32)(
                input
                  .map { s =>
                    (
                      s,
                      buffers.find(
                        _._1 == ra3.lang.ColumnKey(s.tableUniqueId, s.columnIdx)
                      )
                    )
                  }
                  .map {
                    case (_, Some((_, buffer, columnName))) =>
                      IO.pure((Left(buffer), columnName))
                    case (
                          SegmentWithName(segmentParts, _, columnName, _),
                          None
                        ) =>
                      if (maskIsEmpty)
                        IO.pure(
                          (
                            Left(segmentParts.head.tag.makeBufferFromSeq()),
                            columnName
                          )
                        )
                      else if (maskIsComplete && segmentParts.size == 1)
                        IO.pure((Right(segmentParts.head), columnName))
                      else
                        bufferMultiple(segmentParts)
                          .map(b => (Left(b), columnName))
                  }
              )
            r
        })
        .map(_.flatten)

      selected.flatMap { selected =>
        val outputNumElems =
          if (maskIsEmpty) 0 else groupMap.map(_._2).getOrElse(numElems)
        IO.parSequenceN(32)(selected.toList.zipWithIndex.map {
          case ((value, columnName), columnIdx) =>
            val bufferOrSegment = value match {
              case Left(x: Buffer) if x.length == outputNumElems => Left(x)
              case Left(x: Buffer) if outputNumElems == 0 =>
                Left(x.tag.makeBufferFromSeq())
              case Left(_: Buffer) =>
                require(
                  false,
                  "in grouped query you must use an aggregator function on the columns. Use .first to take first item per group"
                )
                ???
              // x.take(BufferInt.apply(Array.fill(outputNumElems)(0)))
              case Right(x: Segment) => Right(x)
              case x: Int    => Left(BufferIntConstant(x, outputNumElems))
              case x: Double => Left(BufferDouble.constant(x, outputNumElems))
              case x: Long   => Left(BufferLong.constant(x, outputNumElems))
              case x: String => Left(BufferString.constant(x, outputNumElems))
              case x => throw new RuntimeException(s"Unknown value returned $x")
            }
            val filteredSegment =
              if (maskIsEmpty)
                bufferOrSegment.swap.toOption.get.tag
                  .makeBufferFromSeq()
                  .toSegment(outputPath.copy(column = columnIdx))
              else if (maskIsComplete && bufferOrSegment.isRight)
                IO.pure(bufferOrSegment.toOption.get)
              else
                mask
                  .map(m => bufferOrSegment.swap.toOption.get.filter(m))
                  .getOrElse(bufferOrSegment.swap.toOption.get)
                  .toSegment(outputPath.copy(column = columnIdx))

            filteredSegment
              .map(s => (s, columnName))
        })
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
    task(SimpleQuery(input, predicate.replaceTags(), outputPath, groupMap))(
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

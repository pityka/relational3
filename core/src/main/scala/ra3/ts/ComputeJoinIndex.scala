package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class ComputeJoinIndex(
    tag: ColumnTag,
    first: Column,
    rest: Seq[(Column, String, Int)], // col, how , index of against which one
    outputPath: LogicalPath
)
private[ra3] object ComputeJoinIndex {
  private def doit(
      tag: ColumnTag,
      first: Column,
      rest: Seq[(Column, String, Int)], // col, how , index of against which one
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[Seq[Option[SegmentInt]]] = {
    scribe.debug(
      f"Compute join index between ${tag}(n=${tag.numElems(first.asInstanceOf[tag.ColumnType])}%,d) and ${rest
          .map { case (col, how, i) =>
            f"${tag}(n=${tag.numElems(col.asInstanceOf[tag.ColumnType])}%,d) $how $i"
          }
          .mkString(" x ")} to $outputPath"
    )
    if (rest.size == 1) {
      val (c, h, _) = rest.head
      doit2(tag)(
        first.asInstanceOf[tag.ColumnType],
        c.asInstanceOf[tag.ColumnType],
        h,
        outputPath
      ).map { case (a, b) => List(a, b) }
    } else
      doitMultiple(tag)(
        first.asInstanceOf[tag.ColumnType],
        rest.map { case (a, b, c) =>
          (a.asInstanceOf[tag.ColumnType], b, c)
        },
        outputPath
      )
  }
  private def doit2(tag: ColumnTag)(
      left: tag.ColumnType,
      right: tag.ColumnType,
      how: String,
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents) = {
    def emptyOverlap() = {
      val rMM = tag.nonMissingMinMax(right)
      val lMM = tag.nonMissingMinMax(left)
      if (rMM.isDefined && lMM.isDefined) {
        val rMin = rMM.get._1
        val rMax = rMM.get._2
        val lMin = lMM.get._1
        val lMax = lMM.get._2
        tag.ordering.lt(rMax, lMin) || tag.ordering.lt(lMax, rMin)
      } else false
    }
    if (how == "inner" && emptyOverlap())
      IO.pure(
        (Some(ColumnTag.I32.emptySegment), Some(ColumnTag.I32.emptySegment))
      )
    else {

      val bufferedLeft = IO
        .parSequenceN(32)(tag.segments(left).map(tag.buffer))
        .map(b => tag.cat(b*))
      val bufferedRight = IO
        .parSequenceN(32)(tag.segments(right).map(tag.buffer))
        .map(b => tag.cat(b*))

      IO.both(bufferedLeft, bufferedRight).flatMap {
        case (bufferedLeft, bufferedRight) =>
          val (takeLeft, takeRight) =
            tag.computeJoinIndexes(bufferedLeft, bufferedRight, how)

          val intTag = ColumnTag.I32
          val takeLeftS: IO[Option[SegmentInt]] =
            takeLeft
              .map(
                intTag
                  .toSegment(
                    _,
                    outputPath
                      .copy(table = outputPath.table + ".left")
                  )
                  .map(_.asInstanceOf[SegmentInt])
                  .map(Some(_))
              )
              .getOrElse(IO.pure(None))
          val takeRightS: IO[Option[SegmentInt]] = takeRight
            .map(
              intTag
                .toSegment(
                  _,
                  outputPath
                    .copy(table = outputPath.table + ".right")
                )
                .map(_.asInstanceOf[SegmentInt])
                .map(Some(_))
            )
            .getOrElse(IO.pure(None))
          IO.both(takeLeftS, takeRightS)
      }
    }
  }
  private def doitMultiple(tag: ColumnTag)(
      first: tag.ColumnType,
      rest: Seq[(tag.ColumnType, String, Int)],
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[Seq[Option[SegmentInt]]] = {
    def emptyOverlap(right: tag.BufferType, left: tag.ColumnType) = {
      val rMM = tag.nonMissingMinMax(right)
      val lMM = tag.nonMissingMinMax(left)
      if (rMM.isDefined && lMM.isDefined) {
        val rMin = rMM.get._1
        val rMax = rMM.get._2
        val lMin = lMM.get._1
        val lMax = lMM.get._2
        tag.ordering.lt(rMax, lMin) || tag.ordering.lt(lMax, rMin)
      } else false
    }

    def continue(
        previousTakes: Seq[(tag.BufferType, Option[BufferInt])],
        nextColumn: tag.ColumnType,
        how: String,
        against: Int
    ): IO[Seq[(tag.BufferType, Option[BufferInt])]] = {
      val bufferedLeft = previousTakes(against)._1
      lazy val bufferedRight = IO
        .parSequenceN(32)(tag.segments(nextColumn).map(tag.buffer))
        .map(b => tag.cat(b*))

      bufferedRight.map { case bufferedRight =>
        if (how == "inner" && emptyOverlap(bufferedLeft, nextColumn)) {
          previousTakes.map { case (b, _) =>
            (tag.take(b, BufferInt.empty), Some(BufferInt.empty))
          } :+ ((tag.makeBufferFromSeq(), Some(BufferInt.empty)))
        } else {
          val (takeLeft, takeRight) =
            tag.computeJoinIndexes(bufferedLeft, bufferedRight, how)

          val updatedPreviousTakes = previousTakes.map {
            case (prevB, None) =>
              takeLeft match {
                case None    => (prevB, None)
                case Some(t) => (tag.take(prevB, t), Some(t))
              }
            case (prevB, Some(t)) =>
              val t1 = takeLeft.map(ColumnTag.I32.take(t, _)).getOrElse(t)
              val b = takeLeft.map(tag.take(prevB, _)).getOrElse(prevB)
              (b, Some(t1))
          }

          val nextRight =
            takeRight.map(tag.take(bufferedRight, _)).getOrElse(bufferedRight)

          updatedPreviousTakes :+ ((nextRight, takeRight))
        }

      }
    }

    val start: IO[Seq[(tag.BufferType, Option[BufferInt])]] = IO
      .parSequenceN(32)(tag.segments(first).map(tag.buffer))
      .map(b => tag.cat(b*))
      .map { b => List((b, Option.empty[BufferInt])) }

    rest
      .foldLeft(start) { case (acc, (next, how, against)) =>
        acc.flatMap { acc =>
          continue(
            acc,
            next,
            how,
            against
          )
        }
      }
      .flatMap { list =>
        IO.parSequenceN(32)(list.zipWithIndex.map { case ((_, take), idx) =>
          take
            .map(
              ColumnTag.I32
                .toSegment(
                  _,
                  outputPath
                    .copy(table = outputPath.table + s".joinindex.$idx")
                )
                .map(Some(_))
            )
            .getOrElse(IO.pure(Option.empty[SegmentInt]))
        })
      }

  }
  def queue(tag: ColumnTag)(
      first: tag.ColumnType,
      rest: Seq[(tag.ColumnType, String, Int)],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Seq[Option[SegmentInt]]] =
    IO {
      scribe.debug(
        s"Queuing ComputeJoinIndex of type $tag with items ${tag.numElems(first)} and ${rest.map(v => tag.numElems(v._1))} sizes"
      )
    } *> task(ComputeJoinIndex(tag, first, rest, outputPath))(
      ResourceRequest(
        cpu = (1, 1),
        memory = (ra3.Utils.guessMemoryUsageInMB(tag)(first) + rest
          .map(_._1)
          .map(ra3.Utils.guessMemoryUsageInMB(tag))
          .sum) * (tag match {
          case x if x == ra3.ColumnTag.StringTag => 16
          case _                                 => 4
        }),
        scratch = 0,
        gpu = 0
      )
    )
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[ComputeJoinIndex] = JsonCodecMaker.make
  implicit val codec2: JsonValueCodec[Seq[(Option[SegmentInt])]] =
    JsonCodecMaker.make
  // $COVERAGE-ON$
  val task =
    Task[ComputeJoinIndex, Seq[(Option[SegmentInt])]](
      "ComputeJoinIndex",
      1
    ) { case input =>
      implicit ce => doit(input.tag, input.first, input.rest, input.outputPath)

    }
}

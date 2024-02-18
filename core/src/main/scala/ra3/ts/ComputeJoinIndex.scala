package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class ComputeJoinIndex(
    first: Column,
    rest: Seq[(Column, String, Int)], // col, how , index of against which one
    outputPath: LogicalPath
)
object ComputeJoinIndex {
  private def doit(
      first: Column,
      rest: Seq[(Column, String, Int)],
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[Seq[Option[SegmentInt]]] = {
    scribe.debug(
      f"Compute join index between ${first.tag}(n=${first.numElems}%,d) and ${rest
          .map { case (col, how, i) =>
            f"${col.tag}(n=${col.numElems}%,d) $how $i"
          }
          .mkString(" x ")} to $outputPath"
    )
    if (rest.size == 1) {
      val (c, h, _) = rest.head
      doit2(first, c, h, outputPath).map { case (a, b) => List(a, b) }
    } else doitMultiple(first, rest, outputPath)
  }
  private def doit2(
      left: Column,
      right: Column,
      how: String,
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents) = {
    def emptyOverlap() = {
      val rightC = right.as(left)
      val rMM = rightC.nonMissingMinMax
      val lMM = left.nonMissingMinMax
      if (rMM.isDefined && lMM.isDefined) {
        val rMin = rMM.get._1
        val rMax = rMM.get._2
        val lMin = lMM.get._1
        val lMax = lMM.get._2
        left.tag.ordering.lt(rMax, lMin) || left.tag.ordering.lt(lMax, rMin)
      } else false
    }
    if (how == "inner" && emptyOverlap())
      IO.pure(
        (Some(ColumnTag.I32.emptySegment), Some(ColumnTag.I32.emptySegment))
      )
    else {

      val bufferedLeft = IO
        .parSequenceN(32)(left.segments.map(_.buffer))
        .map(b => left.tag.cat(b: _*))
      val bufferedRight = IO
        .parSequenceN(32)(right.segments.map(_.buffer))
        .map(b => right.tag.cat(b: _*))

      IO.both(bufferedLeft, bufferedRight).flatMap {
        case (bufferedLeft, bufferedRight) =>
          val (takeLeft, takeRight) =
            bufferedLeft.computeJoinIndexes(bufferedRight.as(bufferedLeft), how)

          val takeLeftS: IO[Option[SegmentInt]] =
            takeLeft
              .map(
                _.toSegment(
                  outputPath
                    .copy(table = outputPath.table + ".left")
                ).map(_.asInstanceOf[SegmentInt]).map(Some(_))
              )
              .getOrElse(IO.pure(None))
          val takeRightS: IO[Option[SegmentInt]] = takeRight
            .map(
              _.toSegment(
                outputPath
                  .copy(table = outputPath.table + ".right")
              ).map(_.asInstanceOf[SegmentInt]).map(Some(_))
            )
            .getOrElse(IO.pure(None))
          IO.both(takeLeftS, takeRightS)
      }
    }
  }
  private def doitMultiple(
      first: Column,
      rest: Seq[(Column, String, Int)],
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[Seq[Option[SegmentInt]]] = {
    def emptyOverlap(right: Buffer, left: Column) = {
      val rightC = right.as(left.tag)
      val rMM = rightC.nonMissingMinMax
      val lMM = left.nonMissingMinMax
      if (rMM.isDefined && lMM.isDefined) {
        val rMin = rMM.get._1
        val rMax = rMM.get._2
        val lMin = lMM.get._1
        val lMax = lMM.get._2
        left.tag.ordering.lt(rMax, lMin) || left.tag.ordering.lt(lMax, rMin)
      } else false
    }

    def continue(
        previousTakes: Seq[(Buffer, Option[BufferInt])],
        nextColumn: Column,
        how: String,
        against: Int
    ): IO[Seq[(Buffer, Option[BufferInt])]] = {
      val bufferedLeft = previousTakes(against)._1
      lazy val bufferedRight = IO
        .parSequenceN(32)(nextColumn.segments.map(_.buffer))
        .map(b => nextColumn.tag.cat(b: _*))

      bufferedRight.map { case bufferedRight =>
        if (how == "inner" && emptyOverlap(bufferedLeft, nextColumn)) {
          previousTakes.map { case (b, _) =>
            (b.take(BufferInt.empty), Some(BufferInt.empty))
          } :+ ((nextColumn.tag.makeBufferFromSeq(), Some(BufferInt.empty)))
        } else {
          val (takeLeft, takeRight) =
            bufferedLeft.computeJoinIndexes(bufferedRight.as(bufferedLeft), how)

          val updatedPreviousTakes = previousTakes.map {
            case (prevB, None) =>
              takeLeft match {
                case None    => (prevB, None)
                case Some(t) => (prevB.take(t), Some(t))
              }
            case (prevB, Some(t)) =>
              val t1 = takeLeft.map(t.take).getOrElse(t)
              val b = takeLeft.map(prevB.take).getOrElse(prevB)
              (b, Some(t1))
          }

          val nextRight =
            takeRight.map(bufferedRight.take).getOrElse(bufferedRight)

          updatedPreviousTakes :+ ((nextRight, takeRight))
        }

      }
    }

    val start: IO[Seq[(Buffer, Option[BufferInt])]] = IO
      .parSequenceN(32)(first.segments.map(_.buffer))
      .map(b => first.tag.cat(b: _*))
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
              _.toSegment(
                outputPath
                  .copy(table = outputPath.table + s".joinindex.$idx")
              ).map(_.asInstanceOf[SegmentInt]).map(Some(_))
            )
            .getOrElse(IO.pure(Option.empty[SegmentInt]))
        })
      }

  }
  def queue[C <: Column](
      first: C,
      rest: Seq[(C, String, Int)],
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Seq[Option[SegmentInt]]] =
    task(ComputeJoinIndex(first, rest, outputPath))(
      ResourceRequest(
        cpu = (1, 1),
        memory = (ra3.Utils.guessMemoryUsageInMB(first) + rest
          .map(_._1)
          .map(ra3.Utils.guessMemoryUsageInMB)
          .sum) *  (first.tag match {
            case x if x == ra3.ColumnTag.StringTag => 16 
            case _ => 4
          }),
        scratch = 0,
        gpu = 0
      )
    )
  implicit val codec: JsonValueCodec[ComputeJoinIndex] = JsonCodecMaker.make
  implicit val codec2: JsonValueCodec[Seq[(Option[SegmentInt])]] =
    JsonCodecMaker.make
  val task =
    Task[ComputeJoinIndex, Seq[(Option[SegmentInt])]](
      "ComputeJoinIndex",
      1
    ) { case input =>
      implicit ce => doit(input.first, input.rest, input.outputPath)

    }
}

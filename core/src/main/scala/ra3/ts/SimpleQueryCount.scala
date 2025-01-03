package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO
import ra3.lang.ReturnValueTuple

private[ra3] case class SimpleQueryCount(
    input: Seq[(ColumnTag, SegmentWithName)],
    predicate: ra3.lang.runtime.Expr,
    groupMap: Option[(SegmentInt, Int)]
)
private[ra3] object SimpleQueryCount {

  private def doit(
      input: Seq[(ColumnTag, SegmentWithName)],
      predicate: ra3.lang.runtime.Expr,
      groupMap: Option[(SegmentInt, Int)]
  )(implicit tsc: TaskSystemComponents): IO[Int] = {
    scribe.debug(
      s"SimpleQueryCount task on ${input
          .groupBy(_._2.tableUniqueId)
          .toSeq
          .map(s => (s._1, s._2.map(v => (v._2.columnName, v._2.segment.size))))} with $predicate . Grouping: $groupMap"
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
    groupMapBuffer.flatMap { case groupMapBuffer =>
      val env1: Map[ra3.lang.Key, ra3.lang.runtime.Value] =
        input
          .map { case (tag, segmentWithName) =>
            val columnKey = ra3.lang.ColumnKey(
              segmentWithName.tableUniqueId,
              segmentWithName.columnIdx
            )
            (
              columnKey,
              ra3.lang.runtime.Value(
                tag.wrap(
                  segmentWithName.segment.asInstanceOf[Seq[tag.SegmentType]]
                )
              )
            )
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

          val maskIsComplete = mask match {
            case None                                        => true
            case Some(Left(BufferIntConstant(1, _)))         => true
            case Some(Right(s)) if s.forall(_.isConstant(1)) => true
            case _                                           => false
          }

          scribe.debug(
            s"SQ program evaluation done projection: ${ReturnValueTuple.list(returnValue)} filter: ${returnValue.filter} maskIsEmpty=$maskIsEmpty maskIsComplete=$maskIsComplete"
          )

          if (maskIsEmpty) IO.pure(0)
          else if (maskIsComplete)
            IO.pure(
              groupMapBuffer.map(_._1.toSeq.distinct.size).getOrElse(numElems)
            )
          else
            mask match {
              case Some(Right(s)) =>
                ra3.Utils
                  .bufferMultiple(ColumnTag.I32)(s)
                  .map(_.positiveLocations.length)
              case Some(Left(b)) => IO.pure(b.positiveLocations.length)
              case None =>
                IO.pure(
                  groupMapBuffer
                    .map(_._1.toSeq.distinct.size)
                    .getOrElse(numElems)
                )
            }
        }

    }.logElapsed
  }
  def queue(
      // (segment, table unique id)
      input: Seq[(ColumnTag, SegmentWithName)],
      predicate: ra3.lang.runtime.Expr,
      groupMap: Option[(SegmentInt, Int)]
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Long] =
    IO {
      scribe.debug(
        s"Queueing SimpleQueryCount on ${input.size} table segments. Groups present: ${groupMap.isDefined}"
      )
    } *> task(
      SimpleQueryCount(input, predicate, groupMap)
    )(
      ResourceRequest(
        cpu = (1, 1),
        memory =
          input.flatMap(_._2.segment).map(ra3.Utils.guessMemoryUsageInMB).sum,
        scratch = 0,
        gpu = 0
      )
    )
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[SimpleQueryCount] = JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[SimpleQueryCount, Long]("SimpleQueryCount", 1) { case input =>
    implicit ce =>
      doit(input.input, input.predicate, input.groupMap).map(_.toLong)

  }
}

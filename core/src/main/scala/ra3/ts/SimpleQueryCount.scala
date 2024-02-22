package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import ra3.lang.ReturnValue

private[ra3] case class SimpleQueryCount(
    input: Seq[SegmentWithName],
    predicate: ra3.lang.Expr,
    groupMap: Option[(SegmentInt, Int)]
)
private[ra3] object SimpleQueryCount {

  def doit(
      input: Seq[SegmentWithName],
      predicate: ra3.lang.Expr,
      groupMap: Option[(SegmentInt, Int)]
  )(implicit tsc: TaskSystemComponents): IO[Int] = {
    scribe.debug(
      s"SimpleQueryCount task on ${input
          .groupBy(_.tableUniqueId)
          .toSeq
          .map(s => (s._1, s._2.map(v => (v.columnName, v.segment.size))))} with $predicate . Grouping: $groupMap"
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

          if (maskIsEmpty) IO.pure(0)
          else if (maskIsComplete)
            IO.pure(
              groupMapBuffer.map(_._1.toSeq.distinct.size).getOrElse(numElems)
            )
          else
            mask match {
              case Some(Right(s)) =>
                ra3.ts.SimpleQuery
                  .bufferMultiple(s)
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

    }
  }
  def queue(
      // (segment, table unique id)
      input: Seq[SegmentWithName],
      predicate: ra3.lang.Expr { type T <: ReturnValue },
      groupMap: Option[(SegmentInt, Int)]
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Long] =
    task(
      SimpleQueryCount(input, predicate.replaceTags(Map.empty), groupMap)
    )(
      ResourceRequest(
        cpu = (1, 1),
        memory =
          input.flatMap(_.segment).map(ra3.Utils.guessMemoryUsageInMB).sum,
        scratch = 0,
        gpu = 0
      )
    )
  implicit val codec: JsonValueCodec[SimpleQueryCount] = JsonCodecMaker.make
  val task = Task[SimpleQueryCount, Long]("SimpleQueryCount", 1) { case input =>
    implicit ce =>
      doit(input.input, input.predicate, input.groupMap).map(_.toLong)

  }
}

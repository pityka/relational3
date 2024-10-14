package ra3
import cats.effect.IO
import tasks.{TaskSystemComponents}
private[ra3] case class UntypedCDF(
    locationsSegment: Segment,
    values: SegmentDouble
) {
  def toTyped(tag: ColumnTag) =
    new TypedCDF(tag, locationsSegment.asInstanceOf[tag.SegmentType], values)
}
private[ra3] class TypedCDF(
    val tag: ColumnTag,
    val locationsSegment: tag.SegmentType,
    val values: SegmentDouble
) {
  /*  Returns a single element which is above or below the required percentile */
  def topK(queryPercentile: Double, ascending: Boolean)(implicit
      tsc: TaskSystemComponents
  ): IO[Option[tag.BufferType]] = {
    // locations.buffer.map(Some(_))
    val locs: IO[tag.BufferType] = tag.buffer(locationsSegment)
    val vals: IO[BufferDouble] = values.buffer
    IO.both(locs, vals).map { case (locsBuffer, vals) =>
      val indexInLocs =
        if (ascending)
          vals.toSeq.zipWithIndex.find(_._1 >= queryPercentile).map(_._2)
        else {

          val s = vals.toSeq
          s.zipWithIndex.reverse.find(_._1 <= (1d - queryPercentile)).map(_._2)
        }
      indexInLocs.map(idx => tag.take(locsBuffer, BufferInt(Array(idx))))
    }

  }
}
private[ra3] object CDF {
  def mergeCDFs[T: Ordering](
      cdfs: Seq[Vector[(T, Double)]]
  ): Vector[(T, Double)] = {

    val locations = cdfs.flatMap(_.map(_._1)).distinct.sorted
    val ord = implicitly[Ordering[T]]
    val pointsAtLocations = cdfs.map { cdf =>
      locations.map { loc =>
        cdf.find(_._1 == loc) match {
          case None =>
            if (ord.lteq(loc, cdf.head._1)) cdf.head._2
            else if (ord.gteq(loc, cdf.last._1)) cdf.last._2
            else {
              val before = cdf.takeWhile(v => ord.lt(v._1, loc)).last._2
              before
            }
          case Some((_, value)) => value
        }
      }
    }
    val aggregated = pointsAtLocations.transpose.map(v => (v.sum) / v.length)
    assert(aggregated.size == locations.size)

    locations.zip(aggregated).toVector
  }
}

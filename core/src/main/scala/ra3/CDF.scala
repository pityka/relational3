package ra3
import cats.effect.IO
import tasks.{TaskSystemComponents}
case class CDF(locations: Segment, values: SegmentInt) {
  def topK(k: Int, ascending: Boolean)(implicit tsc: TaskSystemComponents) = {
    val locs: IO[Buffer] = locations.buffer
    val vals: IO[BufferInt] = values.buffer
    IO.both(locs, vals).map { case (locs, vals) =>
      val indexInLocs =
        if (ascending) vals.toSeq.zipWithIndex.find(_._1 >= k).map(_._2)
        else {

          val s = vals.toSeq
          val max = s.max
          s.zipWithIndex.reverse.find(_._1 <= (max - k)).map(_._2)
        }
      indexInLocs.map(idx => locs.take(BufferInt(Array(idx))))
    }

  }
}
object CDF {
  def mergeCDFs[T: Ordering](
      cdfs: Seq[Vector[(T, Int)]]
  ): Vector[(T, Int)] = {
    // this is wrong. it should operate on quantiles, not on counts

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
              val after = cdf.dropWhile((v => ord.lt(v._1, loc))).head._2
              val mean = ((after + before) * 0.5).toInt
              mean
            }
          case Some((_, value)) => value
        }
      }
    }
    val aggregated = pointsAtLocations.transpose.map(_.sum)
    assert(aggregated.size == locations.size)
    locations.zip(aggregated).toVector
  }
}
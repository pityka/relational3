package ra3

sealed trait Statistic[T] {
  def hasMissing: Boolean
  def minMax: Option[(T, T)]
  def lowCardinalityNonMissingSet: Option[Set[T]]
  def countNonMissing: Int
}

case class StatisticInt(
    hasMissing: Boolean,
    minMax: Option[(Int, Int)],
    lowCardinalityNonMissingSet: Option[Set[Int]],
    countNonMissing: Int
) extends Statistic[Int]

object StatisticInt {
 
}

case class StatisticLong(
    hasMissing: Boolean,
    minMax: Option[(Long, Long)],
    lowCardinalityNonMissingSet: Option[Set[Long]],
    countNonMissing: Int
) extends Statistic[Long]

case class StatisticDouble(
    hasMissing: Boolean,
    minMax: Option[(Double, Double)],
    lowCardinalityNonMissingSet: Option[Set[Double]],
    countNonMissing: Int
) extends Statistic[Double]

case class StatisticCharSequence(
    hasMissing: Boolean,
    minMax: Option[(CharSequence, CharSequence)],
    lowCardinalityNonMissingSet: Option[Set[CharSequence]],
    countNonMissing: Int
) extends Statistic[CharSequence]

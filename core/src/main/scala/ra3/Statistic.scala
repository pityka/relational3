package ra3
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._

sealed trait Statistic[T] {
  def hasMissing: Boolean
  def nonMissingMinMax: Option[(T, T)]
  def lowCardinalityNonMissingSet: Option[Set[T]]
  def countNonMissing: Int
  def constantValue : Option[T]
}


case class StatisticInt(
    hasMissing: Boolean,
    nonMissingMinMax: Option[(Int, Int)],
    lowCardinalityNonMissingSet: Option[Set[Int]],
    countNonMissing: Int
) extends Statistic[Int]  {
  def constantValue = nonMissingMinMax match {
    case Some((a,b)) if a == b => Some(a)
    case Some(_) => None     
    case None if hasMissing => Some(BufferInt.MissingValue)
    case _ => None
  }
}

object StatisticInt {
  def constant(i:Int,length: Int) = StatisticInt(false,Some((i,i)),Some(Set(i)),length)
val empty = StatisticInt(false,None,None,0)
  implicit val segmentCodec: JsonValueCodec[StatisticInt] = JsonCodecMaker.make
}

case class StatisticLong(
    hasMissing: Boolean,
    nonMissingMinMax: Option[(Long, Long)],
    lowCardinalityNonMissingSet: Option[Set[Long]],
    countNonMissing: Int
) extends Statistic[Long] {
   def constantValue = nonMissingMinMax match {
    case Some((a,b)) if a == b => Some(a)
    case Some(_) => None     
    case None if hasMissing => Some(BufferLong.MissingValue)
    case _ => None
  }
}

object StatisticLong {
  val empty = StatisticLong(false,None,None,0)
  implicit val segmentCodec: JsonValueCodec[StatisticLong] = JsonCodecMaker.make
}

case class StatisticDouble(
    hasMissing: Boolean,
    nonMissingMinMax: Option[(Double, Double)],
    lowCardinalityNonMissingSet: Option[Set[Double]],
    countNonMissing: Int
) extends Statistic[Double] {
   def constantValue = nonMissingMinMax match {
    case Some((a,b)) if a == b => Some(a)
    case Some(_) => None     
    case None if hasMissing => Some(BufferDouble.MissingValue)
    case _ => None
  }
}

object StatisticDouble {
  val empty = StatisticDouble(false,None,None,0)
  implicit val customCodecOfDouble: JsonValueCodec[Double] =
    Utils.customDoubleCodec
  implicit val segmentCodec: JsonValueCodec[StatisticDouble] =
    JsonCodecMaker.make
}
case class StatisticCharSequence(
    hasMissing: Boolean,
    nonMissingMinMax: Option[(CharSequence, CharSequence)],
    lowCardinalityNonMissingSet: Option[Set[CharSequence]],
    countNonMissing: Int
) extends Statistic[CharSequence] {
   def constantValue = nonMissingMinMax match {
    case Some((a,b)) if a == b => Some(a)
    case Some(_) => None     
    case None if hasMissing => Some(BufferString.MissingValue)
    case _ => None
  }
}
object StatisticCharSequence {
    val empty = StatisticCharSequence(false,None,None,0)

  implicit val cs: JsonValueCodec[CharSequence] = ra3.Utils.charSequenceCodec
  implicit val segmentCodec: JsonValueCodec[StatisticCharSequence] =
    JsonCodecMaker.make
}

package ra3
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._

case class StatisticInt(
    hasMissing: Boolean,
    nonMissingMinMax: Option[(Int, Int)],
    lowCardinalityNonMissingSet: Option[Set[Int]]
) {
  def mightEq(i: Int): Boolean = if (i == BufferInt.MissingValue)
    false
  else {
    lowCardinalityNonMissingSet match {
      case None =>
        nonMissingMinMax.exists { case (min, max) => i >= min && i <= max }
      case Some(set) => set.contains(i)
    }
  }

  def mightLtEq(i: Int): Boolean = {
    if (i == BufferInt.MissingValue) false
    else nonMissingMinMax.exists { case (min, _) => i >= min }

  }
  def mightLt(i: Int): Boolean = {
    if (i == BufferInt.MissingValue) false
    else nonMissingMinMax.exists { case (min, _) => i > min }

  }
  def mightGtEq(i: Int): Boolean = {
    if (i == BufferInt.MissingValue) false
    else nonMissingMinMax.exists { case (_, max) => i <= max }

  }
  def mightGt(i: Int): Boolean = {
    if (i == BufferInt.MissingValue) false
    else nonMissingMinMax.exists { case (_, max) => i < max }
  }

  def constantValue = nonMissingMinMax match {
    case Some((a, b)) if a == b => Some(a)
    case Some(_)                => None
    case None if hasMissing     => Some(BufferInt.MissingValue)
    case _                      => None
  }
}

object StatisticInt {
  def constant(i: Int) = StatisticInt(false, Some((i, i)), Some(Set(i)))
  val empty = StatisticInt(false, None, None)
  implicit val segmentCodec: JsonValueCodec[StatisticInt] = JsonCodecMaker.make
}

case class StatisticLong(
    hasMissing: Boolean,
    nonMissingMinMax: Option[(Long, Long)],
    lowCardinalityNonMissingSet: Option[Set[Long]],
    bloomFilter: Option[BloomFilter]
) {
  def mightLtEq(i: Long): Boolean = {
    if (i == BufferLong.MissingValue) false
    else nonMissingMinMax.exists { case (min, _) => i >= min }

  }
  def mightLt(i: Long): Boolean = {
    if (i == BufferLong.MissingValue) false
    else nonMissingMinMax.exists { case (min, _) => i > min }

  }
  def mightGtEq(i: Long): Boolean = {
    if (i == BufferLong.MissingValue) false
    else nonMissingMinMax.exists { case (_, max) => i <= max }

  }
  def mightGt(i: Long): Boolean = {
    if (i == BufferLong.MissingValue) false
    else nonMissingMinMax.exists { case (_, max) => i < max }
  }

  def mightEq(i: Long): Boolean =
    if (i == BufferLong.MissingValue)
      false
    else {
      lowCardinalityNonMissingSet match {
        case None =>
          nonMissingMinMax.exists { case (min, max) =>
            val inRange = i >= min && i <= max
            inRange && bloomFilter.map(_.mightContainLong(i)).getOrElse(true)
          }
        case Some(set) => set.contains(i)
      }
    }

  def constantValue = nonMissingMinMax match {
    case Some((a, b)) if a == b => Some(a)
    case Some(_)                => None
    case None if hasMissing     => Some(BufferLong.MissingValue)
    case _                      => None
  }
}

object StatisticLong {
  val empty = StatisticLong(false, None, None, None)
  implicit val segmentCodec: JsonValueCodec[StatisticLong] = JsonCodecMaker.make
}

case class StatisticDouble(
    hasMissing: Boolean,
    nonMissingMinMax: Option[(Double, Double)],
    lowCardinalityNonMissingSet: Option[Set[Double]]
) {

  def mightLtEq(i: Double): Boolean = {
    if (i.isNaN) false
    else nonMissingMinMax.exists { case (min, _) => i >= min }

  }
  def mightLt(i: Double): Boolean = {
    if (i.isNaN) false
    else nonMissingMinMax.exists { case (min, _) => i > min }

  }
  def mightGtEq(i: Double): Boolean = {
    if (i.isNaN) false
    else nonMissingMinMax.exists { case (_, max) => i <= max }

  }
  def mightGt(i: Double): Boolean = {
    if (i.isNaN) false
    else nonMissingMinMax.exists { case (_, max) => i < max }
  }

  def mightEq(i: Double): Boolean = if (i.isNaN)
    false
  else {
    lowCardinalityNonMissingSet match {
      case None =>
        nonMissingMinMax.exists { case (min, max) => i >= min && i <= max }
      case Some(set) => set.contains(i)
    }
  }
  def constantValue = nonMissingMinMax match {
    case Some((a, b)) if a == b => Some(a)
    case Some(_)                => None
    case None if hasMissing     => Some(BufferDouble.MissingValue)
    case _                      => None
  }
}

object StatisticDouble {
  val empty = StatisticDouble(false, None, None)
  implicit val customCodecOfDouble: JsonValueCodec[Double] =
    Utils.customDoubleCodec
  implicit val segmentCodec: JsonValueCodec[StatisticDouble] =
    JsonCodecMaker.make
}
case class StatisticCharSequence(
    hasMissing: Boolean,
    nonMissingMinMax: Option[(CharSequence, CharSequence)],
    lowCardinalityNonMissingSet: Option[Set[CharSequence]],
    bloomFilter: Option[BloomFilter]
) {
  import ra3.bufferimpl.{CharSequenceOrdering => CSO}

  def mightLtEq(i: CharSequence): Boolean = {
    if (i == BufferString.MissingValue) false
    else nonMissingMinMax.exists { case (min, _) => CSO.gteq(i, min) }

  }
  def mightLt(i: CharSequence): Boolean = {
    if (i == BufferString.MissingValue) false
    else nonMissingMinMax.exists { case (min, _) => CSO.gt(i, min) }

  }
  def mightGtEq(i: CharSequence): Boolean = {
    if (i == BufferString.MissingValue) false
    else nonMissingMinMax.exists { case (_, max) => CSO.lteq(i, max) }

  }
  def mightGt(i: CharSequence): Boolean = {
    if (i == BufferString.MissingValue) false
    else nonMissingMinMax.exists { case (_, max) => CSO.lt(i, max) }
  }

  def mightEq(i: CharSequence): Boolean = if (i == BufferString.MissingValue)
    false
  else {
    lowCardinalityNonMissingSet match {
      case None =>
        nonMissingMinMax.exists { case (min, max) =>
          val inRange = ra3.bufferimpl.CharSequenceOrdering
            .gteq(i, min) && ra3.bufferimpl.CharSequenceOrdering.lteq(i, max)
          inRange && bloomFilter
            .map(_.mightContainCharSequence(i))
            .getOrElse(true)

        }
      case Some(set) => set.contains(i)
    }
  }
  def constantValue = nonMissingMinMax match {
    case Some((a, b)) if a == b => Some(a)
    case Some(_)                => None
    case None if hasMissing     => Some(BufferString.MissingValue)
    case _                      => None
  }
}
object StatisticCharSequence {
  val empty = StatisticCharSequence(false, None, None, None)

  implicit val cs: JsonValueCodec[CharSequence] = ra3.Utils.charSequenceCodec
  implicit val segmentCodec: JsonValueCodec[StatisticCharSequence] =
    JsonCodecMaker.make
}

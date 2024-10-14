package ra3.join.util
import java.lang.{Double => JDouble}

private[ra3] class DoubleMap {
  val lmap = new LongMap
  def update(key: Double, value: Int) = {
    lmap.update(JDouble.doubleToLongBits(key), value)
  }
  def get(k: Double) = lmap.get(JDouble.doubleToLongBits(k))

  def foreachKey[A](f: Double => A): Unit = {
    lmap.foreachKey { (l: Long) =>
      f(JDouble.longBitsToDouble(l))
    }
  }
  def contains(k: Double) = lmap.contains(JDouble.doubleToLongBits(k))
}
private[ra3] class DoubleBufferMap {
  val lmap = new LongBufferMap
  def contains(k: Double) = lmap.contains(JDouble.doubleToLongBits(k))
  def update(key: Double, value: Int) = {
    lmap.update(JDouble.doubleToLongBits(key), value)
  }
  def get(k: Double) = lmap.get(JDouble.doubleToLongBits(k))

}

private[ra3] class IntMap {
  val lmap = new LongMap

  def size = lmap.size
  def update(key: Int, value: Int) = {
    lmap.update(key.toLong, value)
  }
  def get(k: Int) = lmap.get(k.toLong)

  def contains(k: Int) = lmap.contains(k)

  def foreachKey[A](f: Int => A): Unit = {
    lmap.foreachKey { (l: Long) =>
      f(l.toInt)
    }
  }
}
private[ra3] class IntBufferMap {
  val lmap = new LongBufferMap

  def contains(k: Int) = lmap.contains(k)
  def update(key: Int, value: Int) = {
    lmap.update(key.toLong, value)
  }
  def get(k: Int) = lmap.get(k.toLong)

  
}

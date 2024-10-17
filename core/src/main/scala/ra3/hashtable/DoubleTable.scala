package ra3.hashtable
import ra3.join.MutableBuffer
import java.lang.Double.{doubleToLongBits as lb}
class DoubleTable(
    longTable: LongTable,
    val payload: Array[Int]
) {
  def contains(q: Double) = longTable.contains(lb(q))
  def lookupAllIdx(q: Double): Array[Int] =
    longTable.lookupAllIdx(lb(q))
  def lookupIdx(q: Double): Int =
    longTable.lookupIdx(lb(q))
  def update(q: Double, v: Int): Unit =
    longTable.update(lb(q), v)
  inline def mutate(q: Double, inline v: Int => Int): Unit =
    longTable.mutate(lb(q), v)

}
object DoubleTable {

  def buildFirst(keys: Array[Double], payload: Array[Int]) = {
    DoubleTable(LongTable.buildFirst(keys.map(lb), payload), payload)
  }
  def buildWithUniques(keys: Array[Double], payload: Array[Int]) = {
    val (a, b) = LongTable.buildWithUniques(keys.map(lb), payload)
    DoubleTable(a, payload) -> b
  }
  def build(keys: Array[Double], payload: Array[Int]) = {
    DoubleTable(LongTable.build(keys.map(lb), payload), payload)
  }

}

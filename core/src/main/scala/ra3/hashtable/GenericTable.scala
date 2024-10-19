package ra3.hashtable
import ra3.join.MutableBuffer
import scala.reflect.ClassTag

private[ra3] class GenericTable[T: ClassTag](
    hashTable: Array[Int],
    keys: Array[T],
    val payload: Array[Int]
) {
  def contains(q: T) = lookupIdx(q) >= 0
  def lookupAllIdx(q: T): Array[Int] =
    GenericTable.lookupAllPayloadIndices(q, hashTable, keys)
  def lookupIdx(q: T): Int =
    GenericTable.lookupPayloadIndex(q, hashTable, keys)
  def update(q: T, v: Int): Unit = {
    val i = GenericTable.lookupPayloadIndex(q, hashTable, keys)
    payload(i) = v
  }
  inline def mutate(q: T, inline v: Int => Int): Unit = {
    val i = GenericTable.lookupPayloadIndex(q, hashTable, keys)
    payload(i) = v(payload(i))
  }

}
private[ra3] object GenericTable {
  inline private def hash[T](cs: T, tableLength: Int): Int = {
    if (cs == null) 0 else math.abs(cs.hashCode) & (tableLength - 1)
  }
  def findLength(l: Int) = {
    var r = 1
    val n = math.ceil(math.log(l * 5d) / math.log(2))
    var i = 0
    while (i < n) {
      r *= 2
      i += 1
    }
    assert(l < r)
    r

  }
  def buildFirst[T: ClassTag](keys: Array[T], payload: Array[Int]) = {
    val l = findLength(keys.length)
    val table = Array.fill(l)(-1)
    var i = 0
    while (i < keys.length) {
      val k = keys(i)
      if (lookupPayloadIndex(k, table, keys) == -1) {
        insert(k, i, table)
      }
      i += 1
    }
    GenericTable(table, keys, payload)
  }
  def buildWithUniques[T: ClassTag](keys: Array[T], payload: Array[Int]) = {
    val l = findLength(keys.length)
    val table = Array.fill(l)(-1)
    var i = 0
    val buffer = ra3.join.MutableBuffer.emptyI
    var misses = 0
    while (i < keys.length) {
      val k = keys(i)
      if (lookupPayloadIndex(k, table, keys) == -1) {
        buffer.+=(i)
        misses += (insert(k, i, table))
      }
      i += 1
    }
    println("Misses: " + misses + " keys: " + keys.length)
    (GenericTable(table, keys, payload), buffer.toArray)
  }
  def build[T: ClassTag](keys: Array[T], payload: Array[Int]) = {
    val table = Array.fill(findLength(keys.length))(-1)
    var i = 0
    var misses = 0
    while (i < keys.length) {
      insert(keys(i), i, table)
      i += 1
    }
    GenericTable(table, keys, payload)
  }
  private def insert[T](k: T, idx: Int, hashTable: Array[Int]) = {
    var h = hash(k, hashTable.length)
    var c = hashTable(h)
    var n = 0
    while (c != -1 && n < hashTable.length) {
      h = (h + 1) & (hashTable.length - 1)
      c = hashTable(h)
      n += 1
    }
    hashTable(h) = idx
    n
  }

  private def lookupPayloadIndex[T](
      query: T,
      hashTable: Array[Int],
      keys: Array[T]
  ) = {
    var h = hash(query, hashTable.length)
    var idx = hashTable(h)
    var n = 0
    while (idx >= 0 && keys(idx) != query && n < hashTable.length) {
      h = (h + 1) & (hashTable.length - 1)
      idx = hashTable(h)
      n += 1
    }
    if (n == hashTable.length && keys(idx) != query) -1
    else idx
  }
  private def lookupAllPayloadIndices[T](
      query: T,
      hashTable: Array[Int],
      keys: Array[T]
  ) = {
    var h = hash(query, hashTable.length)
    var idx = hashTable(h)
    val buffer = MutableBuffer.emptyI
    if (idx == -1) buffer.toArray
    else {
      var n = 0
      while (idx >= 0 && n < hashTable.length) {
        if (keys(idx) == query) {
          buffer.+=(idx)
        }
        h = (h + 1) & (hashTable.length - 1)
        idx = hashTable(h)
        n += 1
      }
      buffer.toArray
    }
  }

}

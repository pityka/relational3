package ra3.hashtable
import ra3.join.MutableBuffer
class IntTable(
    hashTable: Array[Int],
    keys: Array[Int],
    val payload: Array[Int]
) {
  def contains(q: Int) = lookupIdx(q) >= 0
  def lookupAllIdx(q: Int): Array[Int] =
    IntTable.lookupAllPayloadIndices(q, hashTable, keys)
  def lookupIdx(q: Int): Int =
    IntTable.lookupPayloadIndex(q, hashTable, keys)
  def update(q: Int, v: Int): Unit = {
    val i = IntTable.lookupPayloadIndex(q, hashTable, keys)
    payload(i) = v
  }
  inline def mutate(q: Int, inline v: Int => Int): Unit = {
    val i = IntTable.lookupPayloadIndex(q, hashTable, keys)
    payload(i) = v(payload(i))
  }

}
object IntTable {
  inline private def hash(cs: Int, tableLength: Int): Int = {
    math.abs(ra3.Murmur3.hashInt(cs)) & (tableLength - 1)
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
  def buildFirst(keys: Array[Int], payload: Array[Int]) = {
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
    IntTable(table, keys, payload)
  }
  def buildWithUniques(keys: Array[Int], payload: Array[Int]) = {
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
    (IntTable(table, keys, payload), buffer.toArray)
  }
  def build(keys: Array[Int], payload: Array[Int]) = {
    val table = Array.fill(findLength(keys.length))(-1)
    var i = 0
    var misses = 0
    while (i < keys.length) {
      insert(keys(i), i, table)
      i += 1
    }
    IntTable(table, keys, payload)
  }
  private def insert(k: Int, idx: Int, hashTable: Array[Int]) = {
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

  private def lookupPayloadIndex(
      query: Int,
      hashTable: Array[Int],
      keys: Array[Int]
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
  private def lookupAllPayloadIndices(
      query: Int,
      hashTable: Array[Int],
      keys: Array[Int]
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

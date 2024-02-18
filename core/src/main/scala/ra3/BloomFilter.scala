package ra3

import java.nio.charset.StandardCharsets
import com.google.common.hash.HashFunction

private[ra3] class BloomFilterBuilder(
    numBits: Int,
    numHashes: Int
) {
  private val bitset = ra3.BitSet.allocate(numBits)

  private val hf = com.google.common.hash.Hashing.murmur3_128()

  def addLong(l: Long) =
    BloomFilter.hashLong(l, numHashes, hf, bitset, numBits)
  def addCharSequence(l: CharSequence) =
    BloomFilter.hashString(l, numHashes, hf, bitset, numBits)
  def toBloomFilter = BloomFilter(bitset.toBitSet, numHashes)
}

private[ra3] object BloomFilter {

    def hashLong(
      l: Long,
      numHashes: Int,
      h: HashFunction,
      bitset: BitSetBuilder,
      numBits: Int
  ) = {
    deriveMultipleHashes(h.hashLong(l).asLong, numHashes, bitset, numBits)
  }
   def hashString(
      l: CharSequence,
      numHashes: Int,
      h: HashFunction,
      bitset: BitSetBuilder,
      numBits: Int
  ) = {
    deriveMultipleHashes(
      h.hashString(l, StandardCharsets.UTF_8).asLong,
      numHashes,
      bitset,
      numBits
    )
  }

  private def deriveMultipleHashes(
      hash0: Long,
      numHashFunctions: Int,
      bitset: BitSetBuilder,
      numBits: Int
  ) = {
    // split the long into two ints
    val hash1 = hash0.toInt
    val hash2 = (hash0 >>> 32).toInt
    var i = 1
    while (i <= numHashFunctions) {
      bitset.addOne(math.abs(hash1 + i * hash2) % numBits)
      i += 1
    }

  }
  private def deriveMultipleHashesQuery(
      hash0: Long,
      numHashFunctions: Int,
      bitset: BitSet,
      numBits: Int
  ) = {
    // split the long into two ints
    val hash1 = hash0.toInt
    val hash2 = (hash0 >>> 32).toInt
    var i = 1
    var all = true
    while (i <= numHashFunctions) {
      all &= bitset.contains(math.abs(hash1 + i * hash2) % numBits)
      i += 1
    }
    all

  }

  def makeFromLongs(numBits: Int, numHashes: Int, items: Iterable[Long]) = {
    val builder = new BloomFilterBuilder(numBits, numHashes)
    items.foreach { l =>
      builder.addLong(l)
    }
    builder.toBloomFilter
  }
  def makeFromCharSequence(numBits: Int, numHashes: Int, items: Iterable[CharSequence]) = {
    val builder = new BloomFilterBuilder(numBits, numHashes)
    items.foreach { l =>
      builder.addCharSequence(l)
    }
    builder.toBloomFilter
  }
}

private[ra3] case class BloomFilter(bitset: BitSet, numHashes: Int) {

  private val hf = com.google.common.hash.Hashing.murmur3_128()

  private val numBits = bitset.capacity
  def mightContainLong(l: Long) =
    BloomFilter.deriveMultipleHashesQuery(
      hf.hashLong(l).asLong,
      numHashes,
      bitset,
      numBits
    )

  def mightContainCharSequence(l: CharSequence) = {
    BloomFilter.deriveMultipleHashesQuery(
      hf.hashString(l, StandardCharsets.UTF_8).asLong,
      numHashes,
      bitset,
      numBits
    )
  }
}

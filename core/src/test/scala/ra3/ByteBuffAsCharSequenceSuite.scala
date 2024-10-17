package ra3

import java.nio.ByteBuffer
import java.nio.ByteOrder

class ByteBuffAsCharSequenceSuite extends munit.FunSuite {
  test("equals") {

    val s = "abcdefghijk"
    val array1 = {
      val a = s.toCharArray()
      val bb = ByteBuffer.allocate(a.length * 2).order(ByteOrder.BIG_ENDIAN)
      a.foreach(c => bb.putChar(c))
      bb
    }
    val array2 = {
      val a = s.toCharArray()
      val bb = ByteBuffer.allocate(a.length * 2).order(ByteOrder.BIG_ENDIAN)
      a.foreach(c => bb.putChar(c))
      bb
    }
    def toCharArray(cs: CharSequence) = {
      val l = Array.ofDim[Char](cs.length)
      (0 until cs.length foreach { i => l(i) = cs.charAt(i) })
      l.toVector
    }
    val cs1 = new ByteBufferAsCharSequence(array1.slice(6, 12))
    val cs2 = new ByteBufferAsCharSequence(array2.slice(6, 12))
    val cs3 = new ByteBufferAsCharSequence(array2.slice(6, 12))
    val cs4 = new ByteBufferAsCharSequence(array2.slice(4, 14))
    assert(cs1 == cs2)
    assert(cs1 == cs3)
    assert(cs2 == cs3)
    assert(cs4 != cs3)
    assert(cs1.hashCode == cs2.hashCode)
    assert(cs1.hashCode == cs3.hashCode)
    assert(cs4.hashCode != cs3.hashCode)

    assert(cs1.equals("defghi"))
    assert(!cs1.equals("defghiAAA"))
    assert(!cs1.equals(""))
    assert(cs2.equals("defghi"))
    assert(cs3.equals("defghi"))
    assert(cs4.equals("cdefghi"))
    assert(toCharArray(cs1).equals("defghi".toCharArray.toVector))
    assert(toCharArray(cs2).equals("defghi".toCharArray.toVector))
    assert(toCharArray(cs3).equals("defghi".toCharArray.toVector))
    assert(toCharArray(cs4).equals("cdefghi".toCharArray.toVector))

    assert(cs1.toString == "defghi")
    assert(cs2.toString == "defghi")
    assert(cs3.toString == "defghi")
    assert(cs4.toString == "cdefghi")

    assert(Set[CharSequence]("defghi").contains(cs1))
    assert(Set[CharSequence]("defghi").contains(cs2))
    assert(Set[CharSequence]("defghi").contains(cs3))
    assert(!Set[CharSequence]("defghi").contains(cs4))

    assert(!Set[CharSequence]("cdefghi").contains(cs1))
    assert(!Set[CharSequence]("cdefghi").contains(cs2))
    assert(!Set[CharSequence]("cdefghi").contains(cs3))
    assert(Set[CharSequence]("cdefghi").contains(cs4))

  }
}

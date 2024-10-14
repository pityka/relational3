package ra3

class CharArraySubSeqSuite extends munit.FunSuite {
  test("equals") {

    val s = "abcdefghijk"
    val array1 = s.toArray
    val array2 = s.toArray
    val cs1 = new CharArraySubSeq(array1, 3, 9)
    val cs2 = new CharArraySubSeq(array2, 3, 9)
    val cs3 = new CharArraySubSeq(array2, 3, 9)
    val cs4 = new CharArraySubSeq(array2, 2, 9)

    assert(cs1 == cs2)
    assert(cs1 == cs3)
    assert(cs2 == cs3)
    assert(cs4 != cs3)

    assert(cs1.hashCode == cs2.hashCode)
    assert(cs1.hashCode == cs3.hashCode)
    assert(cs4.hashCode != cs3.hashCode)

    assert(cs1.equals("defghi"))
    assert(cs2.equals("defghi"))
    assert(cs3.equals("defghi"))
    assert(cs4.equals("cdefghi"))

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

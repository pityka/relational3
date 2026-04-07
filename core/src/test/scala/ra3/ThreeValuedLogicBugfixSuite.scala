package ra3

class ThreeValuedLogicBugfixSuite extends munit.FunSuite {

  test("constant false AND missing is false") {
    val const0 = BufferInt.constant(0, 3)
    val other = BufferInt(Array(1, Int.MinValue, 0))
    val result = const0.elementwise_&&(other)
    assertEquals(result.toSeq, Seq(0, 0, 0))
  }
  test("constant true OR missing is true") {
    val const1 = BufferInt.constant(1, 3)
    val other = BufferInt(Array(0, Int.MinValue, 1))
    val result = const1.elementwise_||(other)
    assertEquals(result.toSeq, Seq(1, 1, 1))
  }
  test("constant missing AND false is false") {
    val constMissing = BufferInt.constant(Int.MinValue, 2)
    val other = BufferInt(Array(0, 1))
    val result = constMissing.elementwise_&&(other)
    assertEquals(result.values(0), 0)
    assertEquals(result.values(1), BufferInt.MissingValue)
  }
  test("constant missing OR true is true") {
    val constMissing = BufferInt.constant(Int.MinValue, 2)
    val other = BufferInt(Array(1, 0))
    val result = constMissing.elementwise_||(other)
    assertEquals(result.values(0), 1)
    assertEquals(result.values(1), BufferInt.MissingValue)
  }
}

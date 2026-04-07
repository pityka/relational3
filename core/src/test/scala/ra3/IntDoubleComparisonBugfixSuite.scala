package ra3

class IntDoubleComparisonBugfixSuite extends munit.FunSuite {

  test("int eq double does not truncate") {
    val ints = BufferIntInArray(Array(3, 3, 3))
    val doubles = BufferDouble(Array(3.0, 3.7, 2.9))
    val result = ints.elementwise_eq(doubles)
    assertEquals(result.toSeq, Seq(1, 0, 0))
  }
  test("int gteq double does not truncate") {
    val ints = BufferIntInArray(Array(3, 3))
    val doubles = BufferDouble(Array(3.7, 2.9))
    val result = ints.elementwise_gteq(doubles)
    assertEquals(result.toSeq, Seq(0, 1))
  }
  test("int lt double does not truncate") {
    val ints = BufferIntInArray(Array(3, 3))
    val doubles = BufferDouble(Array(3.7, 2.9))
    val result = ints.elementwise_lt(doubles)
    assertEquals(result.toSeq, Seq(1, 0))
  }
  test("int neq double does not truncate") {
    val ints = BufferIntInArray(Array(3, 3))
    val doubles = BufferDouble(Array(3.0, 3.7))
    val result = ints.elementwise_neq(doubles)
    assertEquals(result.toSeq, Seq(0, 1))
  }
}

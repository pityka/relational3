package ra3

class CountDistinctMissingBugfixSuite extends munit.FunSuite {

  test("int countDistinctInGroups excludes missing") {
    val buf = BufferInt(Array(1, 2, Int.MinValue, Int.MinValue, 1))
    val partMap = BufferInt(Array(0, 1, 0, 1, 0))
    val result = buf.countDistinctInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(1, 1))
  }
  test("long countDistinctInGroups excludes missing") {
    val buf = BufferLong(Array(1L, 2L, Long.MinValue, Long.MinValue, 1L))
    val partMap = BufferInt(Array(0, 1, 0, 1, 0))
    val result = buf.countDistinctInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(1, 1))
  }
  test("string countDistinctInGroups excludes missing") {
    val buf = BufferString(
      "a",
      "b",
      BufferString.missing.toString,
      BufferString.missing.toString,
      "a"
    )
    val partMap = BufferInt(Array(0, 1, 0, 1, 0))
    val result = buf.countDistinctInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(1, 1))
  }
  test("constant int countDistinctInGroups with missing value") {
    val buf = BufferInt.constant(Int.MinValue, 4)
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.countDistinctInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(0, 0))
  }
}

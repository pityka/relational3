package ra3

class FirstInGroupSuite extends munit.FunSuite {

  // Note: firstInGroup overwrites as it iterates, so it returns the LAST
  // non-missing value per group, not the first. These tests document that
  // actual behavior.

  test("int firstInGroup") {
    val buf = BufferIntInArray(Array(10, 20, 30, 40))
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assertEquals(result.toSeq, Seq(30, 40))
  }
  test("int firstInGroup skips missing") {
    val buf = BufferIntInArray(Array(10, Int.MinValue, 30, Int.MinValue))
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assertEquals(result.toSeq(0), 30)
    assert(result.isMissing(1)) // group 1 all missing
  }

  test("long firstInGroup") {
    val buf = BufferLong(Array(10L, 20L, 30L, 40L))
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assertEquals(result.toSeq, Seq(30L, 40L))
  }
  test("long firstInGroup skips missing") {
    val buf = BufferLong(Array(10L, Long.MinValue, Long.MinValue, Long.MinValue))
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assertEquals(result.toSeq(0), 10L)
    assert(result.isMissing(1))
  }

  test("double firstInGroup") {
    val buf = BufferDouble(Array(1.5, 2.5, 3.5, 4.5))
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assertEquals(result.toSeq.map(_.toString), Seq("3.5", "4.5"))
  }
  test("double firstInGroup skips missing") {
    val buf = BufferDouble(Array(1.5, Double.NaN, Double.NaN, Double.NaN))
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assertEquals(result.toSeq(0).toString, "1.5")
    assert(result.isMissing(1))
  }

  test("string firstInGroup") {
    val buf = BufferString("a", "b", "c", "d")
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assertEquals(result.toSeq.map(_.toString), Seq("c", "d"))
  }
  test("string firstInGroup skips missing") {
    val m = BufferString.missing.toString
    val buf = BufferString("a", m, m, m)
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assertEquals(result.toSeq(0).toString, "a")
    assert(result.isMissing(1))
  }

  test("instant firstInGroup") {
    val buf = BufferInstant(100L, 200L, 300L, 400L)
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assertEquals(result.toSeq, Seq(300L, 400L))
  }
  test("instant firstInGroup skips missing") {
    val buf = BufferInstant(100L, Long.MinValue, Long.MinValue, Long.MinValue)
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assertEquals(result.toSeq(0), 100L)
    assert(result.isMissing(1))
  }

  test("constant int firstInGroup") {
    val buf = BufferInt.constant(42, 4)
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assertEquals(result.toSeq, Seq(42, 42))
  }
  test("constant int firstInGroup missing") {
    val buf = BufferInt.constant(Int.MinValue, 4)
    val partMap = BufferInt(Array(0, 1, 0, 1))
    val result = buf.firstInGroup(partMap, 2)
    assert(result.isMissing(0))
    assert(result.isMissing(1))
  }
}

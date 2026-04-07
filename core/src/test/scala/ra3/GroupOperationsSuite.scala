package ra3

class GroupOperationsSuite extends munit.FunSuite {

  // allInGroups, anyInGroups, noneInGroups (Int)
  test("allInGroups - all true in group") {
    val buf = BufferIntInArray(Array(1, 1, 1, 1))
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.allInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(1, 1))
  }
  test("allInGroups - one false makes group false") {
    val buf = BufferIntInArray(Array(1, 0, 1, 1))
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.allInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(0, 1))
  }
  test("anyInGroups - one true makes group true") {
    val buf = BufferIntInArray(Array(0, 1, 0, 0))
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.anyInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(1, 0))
  }
  test("noneInGroups - all false means none true") {
    val buf = BufferIntInArray(Array(0, 0, 0, 1))
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.noneInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(1, 0))
  }

  // allInGroups, anyInGroups, noneInGroups (IntConstant)
  test("constant allInGroups - true constant") {
    val buf = BufferInt.constant(1, 4)
    val partMap = BufferInt(Array(0, 0, 1, 1))
    assertEquals(buf.allInGroups(partMap, 2).toSeq, Seq(1, 1))
  }
  test("constant anyInGroups - false constant") {
    val buf = BufferInt.constant(0, 4)
    val partMap = BufferInt(Array(0, 0, 1, 1))
    assertEquals(buf.anyInGroups(partMap, 2).toSeq, Seq(0, 0))
  }
  test("constant noneInGroups - true constant") {
    val buf = BufferInt.constant(1, 4)
    val partMap = BufferInt(Array(0, 0, 1, 1))
    assertEquals(buf.noneInGroups(partMap, 2).toSeq, Seq(0, 0))
  }
  test("constant noneInGroups - false constant") {
    val buf = BufferInt.constant(0, 4)
    val partMap = BufferInt(Array(0, 0, 1, 1))
    assertEquals(buf.noneInGroups(partMap, 2).toSeq, Seq(1, 1))
  }

  // meanInGroups (Int)
  test("int meanInGroups") {
    val buf = BufferIntInArray(Array(2, 4, 10, 20, Int.MinValue))
    val partMap = BufferInt(Array(0, 0, 1, 1, 0))
    val result = buf.meanInGroups(partMap, 2)
    assertEquals(result.values(0), 3.0) // (2+4)/2
    assertEquals(result.values(1), 15.0) // (10+20)/2
  }
  test("int meanInGroups with all-missing group") {
    val buf = BufferIntInArray(Array(Int.MinValue, Int.MinValue, 10))
    val partMap = BufferInt(Array(0, 0, 1))
    val result = buf.meanInGroups(partMap, 2)
    assert(result.values(0).isNaN) // group 0 all missing
    assertEquals(result.values(1), 10.0)
  }

  // meanInGroups (IntConstant)
  test("constant meanInGroups") {
    val buf = BufferInt.constant(5, 4)
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.meanInGroups(partMap, 2)
    assertEquals(result.values(0), 5.0)
    assertEquals(result.values(1), 5.0)
  }
  test("constant meanInGroups with missing constant") {
    val buf = BufferInt.constant(Int.MinValue, 4)
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.meanInGroups(partMap, 2)
    assert(result.isMissing(0))
    assert(result.isMissing(1))
  }

  // meanGroups (Double)
  test("double meanGroups") {
    val buf = BufferDouble(Array(1.0, 3.0, 10.0, 20.0))
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.meanGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(2.0, 15.0))
  }
  test("double meanGroups with missing") {
    val buf = BufferDouble(Array(1.0, Double.NaN, 10.0, Double.NaN))
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.meanGroups(partMap, 2)
    assertEquals(result.toSeq(0), 1.0) // only 1.0 in group 0
    assertEquals(result.toSeq(1), 10.0)
  }
  test("double meanGroups all-missing group") {
    val buf = BufferDouble(Array(Double.NaN, Double.NaN, 5.0))
    val partMap = BufferInt(Array(0, 0, 1))
    val result = buf.meanGroups(partMap, 2)
    assert(result.toSeq(0).isNaN)
    assertEquals(result.toSeq(1), 5.0)
  }

  // countInGroups across types
  test("int countInGroups with missing") {
    val buf = BufferIntInArray(Array(1, Int.MinValue, 3, Int.MinValue))
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.countInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(1, 1))
  }
  test("long countInGroups with missing") {
    val buf = BufferLong(Array(1L, Long.MinValue, 3L, Long.MinValue))
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.countInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(1, 1))
  }
  test("string countInGroups with missing") {
    val buf = BufferString("a", BufferString.missing.toString, "c", BufferString.missing.toString)
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.countInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(1, 1))
  }
  test("instant countInGroups with missing") {
    val buf = BufferInstant(100L, Long.MinValue, 300L, Long.MinValue)
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.countInGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(1, 1))
  }
  test("double countGroups with missing") {
    val buf = BufferDouble(Array(1.0, Double.NaN, 3.0, Double.NaN))
    val partMap = BufferInt(Array(0, 0, 1, 1))
    val result = buf.countGroups(partMap, 2)
    assertEquals(result.toSeq, Seq(1.0, 1.0))
  }

  // countInGroups all-missing group
  test("int countInGroups all-missing group returns 0") {
    val buf = BufferIntInArray(Array(Int.MinValue, Int.MinValue, 5))
    val partMap = BufferInt(Array(0, 0, 1))
    val result = buf.countInGroups(partMap, 2)
    assertEquals(result.toSeq(0), 0) // all missing => count 0
    assertEquals(result.toSeq(1), 1)
  }
}

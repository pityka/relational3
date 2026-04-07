package ra3

class ChooseSuite extends munit.FunSuite {

  // predicate: 1=true, 0=false, MinValue=missing
  private val pred = BufferIntInArray(Array(1, 0, Int.MinValue))

  test("choose Int buffer, buffer") {
    val t = BufferIntInArray(Array(10, 20, 30))
    val f = BufferIntInArray(Array(40, 50, 60))
    val result = pred.elementwise_choose(t, f)
    assertEquals(result.values(0), 10)
    assertEquals(result.values(1), 50)
    assertEquals(result.values(2), Int.MinValue)
  }
  test("choose Int scalar, scalar") {
    val result = pred.elementwise_choose(10, 20)
    assertEquals(result.values(0), 10)
    assertEquals(result.values(1), 20)
    assertEquals(result.values(2), Int.MinValue)
  }
  test("choose Int scalar, buffer") {
    val f = BufferIntInArray(Array(40, 50, 60))
    val result = pred.elementwise_choose(10, f)
    assertEquals(result.values(0), 10)
    assertEquals(result.values(1), 50)
    assertEquals(result.values(2), Int.MinValue)
  }
  test("choose Int buffer, scalar") {
    val t = BufferIntInArray(Array(10, 20, 30))
    val result = pred.elementwise_choose(t, 99)
    assertEquals(result.values(0), 10)
    assertEquals(result.values(1), 99)
    assertEquals(result.values(2), Int.MinValue)
  }

  test("choose Double buffer, buffer") {
    val t = BufferDouble(Array(1.1, 2.2, 3.3))
    val f = BufferDouble(Array(4.4, 5.5, 6.6))
    val result = pred.elementwise_choose(t, f)
    assertEquals(result.values(0), 1.1)
    assertEquals(result.values(1), 5.5)
    assert(result.isMissing(2))
  }
  test("choose Double scalar, scalar") {
    val result = pred.elementwise_choose(1.0, 2.0)
    assertEquals(result.values(0), 1.0)
    assertEquals(result.values(1), 2.0)
    assert(result.isMissing(2))
  }

  test("choose Long buffer, buffer") {
    val t = BufferLong(Array(10L, 20L, 30L))
    val f = BufferLong(Array(40L, 50L, 60L))
    val result = pred.elementwise_choose(t, f)
    assertEquals(result.values(0), 10L)
    assertEquals(result.values(1), 50L)
    assert(result.isMissing(2))
  }
  test("choose Long scalar, scalar") {
    val result = pred.elementwise_choose(10L, 20L)
    assertEquals(result.values(0), 10L)
    assertEquals(result.values(1), 20L)
    assert(result.isMissing(2))
  }

  test("choose String buffer, buffer") {
    val t = BufferString("a", "b", "c")
    val f = BufferString("x", "y", "z")
    val result = pred.elementwise_choose(t, f)
    assertEquals(result.values(0).toString, "a")
    assertEquals(result.values(1).toString, "y")
    assert(result.isMissing(2))
  }
  test("choose String scalar, scalar") {
    val result = pred.elementwise_choose("yes", "no")
    assertEquals(result.values(0).toString, "yes")
    assertEquals(result.values(1).toString, "no")
    assert(result.isMissing(2))
  }

  test("choose Instant buffer, buffer") {
    val t = BufferInstant(100L, 200L, 300L)
    val f = BufferInstant(400L, 500L, 600L)
    val result = pred.elementwise_choose(t, f)
    assertEquals(result.values(0), 100L)
    assertEquals(result.values(1), 500L)
    assert(result.isMissing(2))
  }
}

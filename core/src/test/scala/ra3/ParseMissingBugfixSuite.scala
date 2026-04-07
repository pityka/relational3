package ra3

class ParseMissingBugfixSuite extends munit.FunSuite {

  test("parseDouble missing produces Double NaN not Int.MinValue") {
    val buf = BufferString("3.14", BufferString.missing.toString, "2.0")
    val result = buf.elementwise_parseDouble
    assert(!result.isMissing(0))
    assert(result.isMissing(1), "missing string should produce missing double")
    assert(!result.isMissing(2))
    assert(result.values(1).isNaN, "missing double should be NaN")
  }

  test("parseLong missing produces Long.MinValue not Int.MinValue") {
    val buf = BufferString("42", BufferString.missing.toString, "99")
    val result = buf.elementwise_parseLong
    assert(!result.isMissing(0))
    assert(result.isMissing(1), "missing string should produce missing long")
    assert(!result.isMissing(2))
    assertEquals(result.values(1), Long.MinValue)
  }
}

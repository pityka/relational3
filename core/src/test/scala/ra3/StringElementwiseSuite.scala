package ra3

class StringElementwiseSuite extends munit.FunSuite {

  test("elementwise_concatenate with scalar") {
    val buf = BufferString("hello", "world", BufferString.missing.toString)
    val result = buf.elementwise_concatenate("!")
    assertEquals(result.values(0).toString, "hello!")
    assertEquals(result.values(1).toString, "world!")
    assert(result.isMissing(2))
  }
  test("elementwise_concatenate with buffer") {
    val a = BufferString("a", "b", BufferString.missing.toString, "d")
    val b = BufferString("1", BufferString.missing.toString, "3", "4")
    val result = a.elementwise_concatenate(b)
    assertEquals(result.values(0).toString, "a1")
    assert(result.isMissing(1))
    assert(result.isMissing(2))
    assertEquals(result.values(3).toString, "d4")
  }

  test("elementwise_length") {
    val buf = BufferString("abc", "", "hi", BufferString.missing.toString)
    val result = buf.elementwise_length
    assertEquals(result.toSeq, Seq(3, 0, 2, Int.MinValue))
  }

  test("elementwise_nonempty") {
    val buf =
      BufferString("abc", "", BufferString.missing.toString)
    val result = buf.elementwise_nonempty
    assertEquals(result.toSeq, Seq(1, 0, 0))
  }

  test("elementwise_substring") {
    val buf = BufferString("abcdef", "ghijkl", BufferString.missing.toString)
    val result = buf.elementwise_substring(1, 3)
    assertEquals(result.values(0).toString, "bcd")
    assertEquals(result.values(1).toString, "hij")
    assert(result.isMissing(2))
  }

  test("elementwise_matches") {
    val buf = BufferString("foo123", "bar456", "baz", BufferString.missing.toString)
    val result = buf.elementwise_matches(".*\\d+.*")
    assertEquals(result.values(0), 1)
    assertEquals(result.values(1), 1)
    assertEquals(result.values(2), 0)
    assertEquals(result.values(3), Int.MinValue)
  }

  test("elementwise_matches_replace") {
    val buf = BufferString("foo123", "bar456", "baz", BufferString.missing.toString)
    val result = buf.elementwise_matches_replace(".*\\d+.*", "HAS_DIGITS")
    assertEquals(result.values(0).toString, "HAS_DIGITS")
    assertEquals(result.values(1).toString, "HAS_DIGITS")
    assertEquals(result.values(2).toString, "baz")
    assert(result.isMissing(3))
  }

  test("elementwise_parseInstant") {
    val buf = BufferString(
      "2020-06-15T12:30:00Z",
      BufferString.missing.toString
    )
    val result = buf.elementwise_parseInstant
    assertEquals(
      result.values(0),
      java.time.Instant.parse("2020-06-15T12:30:00Z").toEpochMilli()
    )
    assert(result.isMissing(1))
  }
}

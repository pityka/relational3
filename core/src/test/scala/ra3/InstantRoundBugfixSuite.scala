package ra3

class InstantRoundBugfixSuite extends munit.FunSuite {

  test("roundToDay uses actual value not Instant.now") {
    val ts = java.time.Instant.parse("2020-06-15T12:30:45Z").toEpochMilli()
    val expected =
      java.time.Instant.parse("2020-06-15T00:00:00Z").toEpochMilli()
    val buf = BufferInstant(ts, Long.MinValue, ts)
    val result = buf.elementwise_roundToDay
    assertEquals(result.values(0), expected)
    assertEquals(result.values(1), BufferInstant.MissingValue)
    assertEquals(result.values(2), expected)
  }
  test("roundToHours uses actual value not Instant.now") {
    val ts = java.time.Instant.parse("2020-06-15T12:30:45Z").toEpochMilli()
    val expected =
      java.time.Instant.parse("2020-06-15T12:00:00Z").toEpochMilli()
    val result = BufferInstant(ts).elementwise_roundToHours
    assertEquals(result.values(0), expected)
  }
  test("roundToMinute uses actual value not Instant.now") {
    val ts = java.time.Instant.parse("2020-06-15T12:30:45Z").toEpochMilli()
    val expected =
      java.time.Instant.parse("2020-06-15T12:30:00Z").toEpochMilli()
    val result = BufferInstant(ts).elementwise_roundToMinute
    assertEquals(result.values(0), expected)
  }
  test("roundToSecond uses actual value not Instant.now") {
    val ts = java.time.Instant
      .parse("2020-06-15T12:30:45.123Z")
      .toEpochMilli()
    val expected =
      java.time.Instant.parse("2020-06-15T12:30:45Z").toEpochMilli()
    val result = BufferInstant(ts).elementwise_roundToSecond
    assertEquals(result.values(0), expected)
  }
}

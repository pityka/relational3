package ra3

class InstantExtractionSuite extends munit.FunSuite {

  // 2020-03-15T14:30:45.123Z
  private val ts =
    java.time.Instant.parse("2020-03-15T14:30:45.123Z").toEpochMilli()

  test("elementwise_years") {
    val buf = BufferInstant(ts, Long.MinValue)
    val result = buf.elementwise_years
    assertEquals(result.values(0), 2020)
    assertEquals(result.values(1), Int.MinValue)
  }
  test("elementwise_months") {
    val buf = BufferInstant(ts, Long.MinValue)
    val result = buf.elementwise_months
    assertEquals(result.values(0), 3)
    assertEquals(result.values(1), Int.MinValue)
  }
  test("elementwise_days") {
    val buf = BufferInstant(ts, Long.MinValue)
    val result = buf.elementwise_days
    assertEquals(result.values(0), 15)
    assertEquals(result.values(1), Int.MinValue)
  }
  test("elementwise_hours") {
    val buf = BufferInstant(ts, Long.MinValue)
    val result = buf.elementwise_hours
    assertEquals(result.values(0), 14)
    assertEquals(result.values(1), Int.MinValue)
  }
  test("elementwise_minutes") {
    val buf = BufferInstant(ts, Long.MinValue)
    val result = buf.elementwise_minutes
    assertEquals(result.values(0), 30)
    assertEquals(result.values(1), Int.MinValue)
  }
  test("elementwise_seconds") {
    val buf = BufferInstant(ts, Long.MinValue)
    val result = buf.elementwise_seconds
    assertEquals(result.values(0), 45)
    assertEquals(result.values(1), Int.MinValue)
  }
  test("elementwise_nanoseconds") {
    val buf = BufferInstant(ts, Long.MinValue)
    val result = buf.elementwise_nanoseconds
    // 123ms = 123_000_000 ns
    assertEquals(result.values(0), 123000000)
    assertEquals(result.values(1), Int.MinValue)
  }

  test("extraction on epoch zero") {
    val zero = BufferInstant(0L)
    assertEquals(zero.elementwise_years.values(0), 1970)
    assertEquals(zero.elementwise_months.values(0), 1)
    assertEquals(zero.elementwise_days.values(0), 1)
    assertEquals(zero.elementwise_hours.values(0), 0)
    assertEquals(zero.elementwise_minutes.values(0), 0)
    assertEquals(zero.elementwise_seconds.values(0), 0)
  }
}

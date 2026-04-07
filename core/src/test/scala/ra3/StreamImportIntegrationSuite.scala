package ra3

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.Duration
import java.time.Instant

class StreamImportIntegrationSuite
    extends munit.FunSuite
    with WithTempTaskSystem {
  override val munitTimeout = Duration(180, "s")

  test("import tuples via stream and query back") {
    withTempTaskSystem { implicit tsc =>
      val data = (1 to 100).map(i => (s"key${i % 5}", i.toDouble))

      val table = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](data.iterator, 32),
          uniqueId = "stream1",
          minimumSegmentSize = 20,
          maximumSegmentSize = 50
        )
        .unsafeRunSync()
        .rename[("key", "value")]

      val result = table.schema { t =>
        query(ra3.all(t).where(t.value > 50.0))
      }.evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()

      val expected =
        data.filter(_._2 > 50.0).map(v => (v._1, v._2)).toSet
      val actual = result.map(v => (v.key.toString, v.value)).toSet
      assertEquals(actual, expected)
    }
  }

  test("import with all five column types") {
    withTempTaskSystem { implicit tsc =>
      val now = Instant.parse("2020-01-01T00:00:00Z")
      val data = Seq(
        (42, 100L, 3.14, "hello", now),
        (0, -1L, Double.NaN, s"${Char.MinValue}", Instant.ofEpochMilli(Long.MinValue)),
        (7, 200L, 2.72, "world", now.plusSeconds(3600))
      )

      val table = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](data.iterator, 256),
          uniqueId = "stream5types",
          minimumSegmentSize = 1,
          maximumSegmentSize = 100
        )
        .unsafeRunSync()
        .rename[("i", "l", "d", "s", "t")]

      val result = table.schema { t =>
        query(ra3.all(t).where(t.i > 0))
      }.evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()

      assertEquals(result.size, 2)
      val sorted = result.sortBy(_.i)
      assertEquals(sorted(0).i, 7)
      assertEquals(sorted(0).l, 200L)
      assertEquals(sorted(1).i, 42)
      assertEquals(sorted(1).s.toString, "hello")
    }
  }

  test("import empty stream produces empty table") {
    withTempTaskSystem { implicit tsc =>
      val data = Seq.empty[(String, Int)]

      val table = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](data.iterator, 256),
          uniqueId = "streamEmpty",
          minimumSegmentSize = 1,
          maximumSegmentSize = 100
        )
        .unsafeRunSync()
        .rename[("key", "value")]

      val result = table.schema { t =>
        query(ra3.all(t))
      }.evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()

      assertEquals(result.size, 0)
    }
  }

  test("multi-segment import preserves all rows") {
    withTempTaskSystem { implicit tsc =>
      val data = (1 to 500).map(i => (s"k$i", i.toLong))

      val table = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](data.iterator, 32),
          uniqueId = "streamMultiSeg",
          minimumSegmentSize = 50,
          maximumSegmentSize = 100
        )
        .unsafeRunSync()
        .rename[("key", "value")]

      val result = table.schema { t =>
        query(ra3.all(t))
      }.evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()

      assertEquals(result.size, 500)
      val values = result.map(_.value).sorted
      assertEquals(values, (1L to 500L).toList)
    }
  }
}

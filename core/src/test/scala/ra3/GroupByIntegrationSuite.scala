package ra3

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.Duration

class GroupByIntegrationSuite extends munit.FunSuite with WithTempTaskSystem {
  override val munitTimeout = Duration(180, "s")

  test("groupBy sum/count/min/max on doubles") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """key,value
          |a,1.0
          |b,2.0
          |a,3.0
          |b,4.0
          |a,5.0
          |c,10.0""".stripMargin

      val table = csvStringToHeterogeneousTable(
        "grp1",
        csv,
        List(
          (0, ColumnTag.StringTag, None, None),
          (1, ColumnTag.F64, None, None)
        ),
        100
      )

      val result = table
        .as[("k", "v"), (StrVar, F64Var)]
        .schema { t =>
          t.k.groupBy.reduceTotal(
            t.k.first.as("k") :*
              t.v.sum.as("sum") :*
              t.v.count.as("count") :*
              t.v.min.as("min") :*
              t.v.max.as("max")
          )
        }
        .evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()
        .map(v => (v.k.toString, v.sum, v.count, v.min, v.max))
        .toSet

      assertEquals(
        result,
        Set(
          ("a", 9.0, 3.0, 1.0, 5.0),
          ("b", 6.0, 2.0, 2.0, 4.0),
          ("c", 10.0, 1.0, 10.0, 10.0)
        )
      )
    }
  }

  test("groupBy with missing values in aggregation column") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """key,value
          |a,1.0
          |a,
          |a,3.0
          |b,
          |b,""".stripMargin

      val table = csvStringToHeterogeneousTable(
        "grp2",
        csv,
        List(
          (0, ColumnTag.StringTag, None, None),
          (1, ColumnTag.F64, None, None)
        ),
        100
      )

      val result = table
        .as[("k", "v"), (StrVar, F64Var)]
        .schema { t =>
          t.k.groupBy.reduceTotal(
            t.k.first.as("k") :*
              t.v.sum.as("sum") :*
              t.v.count.as("count")
          )
        }
        .evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()
        .map(v => (v.k.toString, f"${v.sum}%.1f", v.count))
        .toSet

      // group a: sum=4.0, count=2; group b: all missing => NaN sum, count=0
      assertEquals(
        result,
        Set(
          ("a", "4.0", 2.0),
          ("b", "NaN", 0.0)
        )
      )
    }
  }

  test("groupBy on int column via stream import") {
    withTempTaskSystem { implicit tsc =>
      val data = Seq(
        ("a", "x", 1),
        ("a", "y", 2),
        ("a", "x", 3),
        ("b", "z", 4),
        ("b", "z", 5)
      )

      val table = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](data.iterator, 256),
          uniqueId = "grp3",
          minimumSegmentSize = 10,
          maximumSegmentSize = 100
        )
        .unsafeRunSync()
        .rename[("k", "cat", "amt")]

      val result = table
        .schema { t =>
          t.k.groupBy.reduceTotal(
            t.k.first.as("k") :*
              t.amt.sum.as("sum")
          )
        }
        .evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()
        .map(v => (v.k.toString, v.sum))
        .toSet

      assertEquals(result, Set(("a", 6), ("b", 9)))
    }
  }
}

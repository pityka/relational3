package ra3

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.Duration

class ExpressionIntegrationSuite
    extends munit.FunSuite
    with WithTempTaskSystem {
  override val munitTimeout = Duration(180, "s")

  test("ifelse expression") {
    withTempTaskSystem { implicit tsc =>
      val data = Seq(("a", 10.0), ("b", -5.0), ("c", 0.0))

      val table = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](data.iterator, 256),
          uniqueId = "ifelse1",
          minimumSegmentSize = 10,
          maximumSegmentSize = 100
        )
        .unsafeRunSync()
        .rename[("key", "value")]

      val result = table.schema { t =>
        query(
          (t.key.as("key") :*
            (t.value > 0.0).ifelse(t.value, 0.0).as("clamped"))
        )
      }.evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()
        .map(v => (v.key.toString, v.clamped))
        .toSet

      assertEquals(
        result,
        Set(("a", 10.0), ("b", 0.0), ("c", 0.0))
      )
    }
  }

  test("string concatenation in projection") {
    withTempTaskSystem { implicit tsc =>
      val data = Seq(("hello", "world"), ("foo", "bar"))

      val table = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](data.iterator, 256),
          uniqueId = "concat1",
          minimumSegmentSize = 10,
          maximumSegmentSize = 100
        )
        .unsafeRunSync()
        .rename[("a", "b")]

      val result = table.schema { t =>
        query(
          t.a.as("a") :*
            t.a.concatenate(t.b).as("ab")
        )
      }.evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()
        .map(v => (v.a.toString, v.ab.toString))
        .toSet

      assertEquals(
        result,
        Set(("hello", "helloworld"), ("foo", "foobar"))
      )
    }
  }

  test("arithmetic expressions: multiply and divide") {
    withTempTaskSystem { implicit tsc =>
      val data = Seq((10.0, 3.0), (20.0, 4.0), (30.0, 5.0))

      val table = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](data.iterator, 256),
          uniqueId = "arith1",
          minimumSegmentSize = 10,
          maximumSegmentSize = 100
        )
        .unsafeRunSync()
        .rename[("a", "b")]

      val result = table.schema { t =>
        query(
          (t.a * t.b).as("product") :*
            (t.a / t.b).as("quotient")
        )
      }.evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()
        .map(v => (v.product, f"${v.quotient}%.2f"))
        .toSet

      assertEquals(
        result,
        Set(
          (30.0, "3.33"),
          (80.0, "5.00"),
          (150.0, "6.00")
        )
      )
    }
  }

  test("chained where filters") {
    withTempTaskSystem { implicit tsc =>
      val data = (1 to 20).map(i => (i, i * 10))

      val table = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](data.iterator, 256),
          uniqueId = "chain1",
          minimumSegmentSize = 10,
          maximumSegmentSize = 100
        )
        .unsafeRunSync()
        .rename[("a", "b")]

      val result = table.schema { t =>
        query(ra3.all(t).where(t.a > 5 && t.a <= 15))
      }.evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()
        .map(_.a)
        .sorted

      assertEquals(result, (6 to 15).toList)
    }
  }

  test("isMissing filter") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """a,b
          |1,10.0
          |2,
          |3,30.0
          |4,""".stripMargin

      val table = csvStringToHeterogeneousTable(
        "ismiss",
        csv,
        List(
          (0, ColumnTag.I32, None, None),
          (1, ColumnTag.F64, None, None)
        ),
        100
      )

      val result = table
        .as[("a", "b"), (I32Var, F64Var)]
        .schema { t =>
          query(ra3.select0.extend(t.a.as("a")).where(t.b.isMissing))
        }
        .evaluate
        .unsafeRunSync()

      val rows = result.bufferStream.compile.toList
        .unsafeRunSync()
        .flatMap(_.columns.head.toSeq.asInstanceOf[Seq[Int]])

      assertEquals(rows.toSet, Set(2, 4))
    }
  }

  test("type conversion: string toDouble in filter") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """label,amount
          |a,100
          |b,50
          |c,200""".stripMargin

      val table = csvStringToStringTable("conv1", csv, 2, 100)

      val result = table
        .as[("label", "amount"), (StrVar, StrVar)]
        .schema { t =>
          query(
            ra3.select0.extend(t.label.as("label")).where(t.amount.toDouble > 75.0)
          )
        }
        .evaluate
        .unsafeRunSync()

      val rows = toFrame2(result, ColumnTag.StringTag)
        .colAt(0)
        .toVec
        .toSeq
        .map(_.toString)
        .toSet

      assertEquals(rows, Set("a", "c"))
    }
  }
}

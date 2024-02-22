package ra3
import cats.effect.unsafe.implicits.global

class CSVSuite extends munit.FunSuite with WithTempTaskSystem {
  test("heterogeneous csv") {
    val csvText = """hint,hfloat,htime,hbool,htext
1,1.5,2020-01-01T00:00:00Z,1,"something, something"
2,2.5,2021-01-01T00:00:00Z,0,"something,"
2,3.0,2021-01-01T00:00:00Z,0,"a,""""
    withTempTaskSystem { implicit tsc =>
      val table = csvStringToHeterogeneousTable(
        "heterogeneouscsvtest",
        csvText,
        List(
          (0, ColumnTag.I32, None, None),
          (1, ColumnTag.F64, None, None),
          (2, ColumnTag.Instant, None, None),
          (3, ColumnTag.I64, None, None),
          (4, ColumnTag.StringTag, None, None)
        ),
        100
      )

      assertEquals(
        toFrame2(table.selectColumns(0).unsafeRunSync(), ColumnTag.I32)
          .colAt(0)
          .values
          .toSeq,
        Seq(1, 2, 2)
      )

      assertEquals(
        toFrame2(table.selectColumns(1).unsafeRunSync(), ColumnTag.F64)
          .colAt(0)
          .values
          .toSeq,
        Seq(1.5, 2.5, 3.0)
      )

      assertEquals(
        toFrame2(table.selectColumns(2).unsafeRunSync(), ColumnTag.Instant)
          .colAt(0)
          .values
          .toSeq,
        Seq(
          java.time.Instant.parse("2020-01-01T00:00:00Z").toEpochMilli(),
          java.time.Instant.parse("2021-01-01T00:00:00Z").toEpochMilli(),
          java.time.Instant.parse("2021-01-01T00:00:00Z").toEpochMilli()
        )
      )

      assertEquals(
        toFrame2(table.selectColumns(3).unsafeRunSync(), ColumnTag.I64)
          .colAt(0)
          .values
          .toSeq,
        Seq(1L, 0, 0)
      )

      assertEquals(
        toFrame2(table.selectColumns(4).unsafeRunSync(), ColumnTag.StringTag)
          .colAt(0)
          .values
          .toSeq
          .map(_.toString),
        Seq("something, something", "something,", "a,")
      )

    }
  }
  test("heterogeneous csv subset") {
    val csvText = """hint,hfloat,htime,hbool,htext
1,1.5,2020-01-01T00:00:00Z,1,"something, something"
2,2.5,2021-01-01T00:00:00Z,0,"something,"
2,3.0,2021-01-01T00:00:00Z,0,"a,""""
    withTempTaskSystem { implicit tsc =>
      val table = csvStringToHeterogeneousTable(
        "heterogeneouscsvtest",
        csvText,
        List(
          (1, ColumnTag.F64, None, None),
          (2, ColumnTag.Instant, None, None),
          (3, ColumnTag.I64, None, None),
          (4, ColumnTag.StringTag, None, None)
        ),
        100
      )

      assertEquals(
        toFrame2(table.selectColumns(0).unsafeRunSync(), ColumnTag.F64)
          .colAt(0)
          .values
          .toSeq,
        Seq(1.5, 2.5, 3.0)
      )

      assertEquals(
        toFrame2(table.selectColumns(3).unsafeRunSync(), ColumnTag.StringTag)
          .colAt(0)
          .values
          .toSeq
          .map(_.toString),
        Seq("something, something", "something,", "a,")
      )

    }
  }
  test("heterogeneous csv subset with na") {
    val csvText = """hint,hfloat,htime,hbool,htext
1,NaN,2020-01-01T00:00:00Z,1,"something, something"
2,2.5,2021-01-01T00:00:00Z,0,"something,"
2,3.0,2021-01-01T00:00:00Z,0,"NA""""
    withTempTaskSystem { implicit tsc =>
      val table = csvStringToHeterogeneousTable(
        "heterogeneouscsvtest",
        csvText,
        List(
          (1, ColumnTag.F64, None, None),
          (2, ColumnTag.Instant, None, None),
          (3, ColumnTag.I64, None, None),
          (4, ColumnTag.StringTag, None, Some("NA"))
        ),
        100
      )

      assertEquals(
        toFrame2(table.selectColumns(0).unsafeRunSync(), ColumnTag.F64)
          .colAt(0)
          .values
          .toSeq
          .toList
          .toString,
        List(Double.NaN, 2.5, 3.0).toString
      )

      assertEquals(
        toFrame2(table.selectColumns(3).unsafeRunSync(), ColumnTag.StringTag)
          .colAt(0)
          .values
          .toSeq
          .map(_.toString),
        Seq("something, something", "something,", "\u0000")
      )

    }
  }
}

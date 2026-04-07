package ra3

import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.Duration

class CsvRoundtripSuite extends munit.FunSuite with WithTempTaskSystem {
  override val munitTimeout = Duration(180, "s")

  test("heterogeneous CSV with all types") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """i,l,d,s,inst
          |1,100,1.5,hello,2020-01-01T00:00:00Z
          |2,200,2.5,world,2020-06-15T12:00:00Z
          |3,300,3.5,foo,2021-01-01T00:00:00Z""".stripMargin

      val table = csvStringToHeterogeneousTable(
        "allTypes",
        csv,
        List(
          (0, ColumnTag.I32, None, None),
          (1, ColumnTag.I64, None, None),
          (2, ColumnTag.F64, None, None),
          (3, ColumnTag.StringTag, None, None),
          (4, ColumnTag.Instant, Some(ra3.InstantParser.ISO), None)
        ),
        100
      )

      assertEquals(table.numRows, 3L)
      assertEquals(table.numCols, 5)

      val buf = table.bufferSegment(0).unsafeRunSync()
      val ints = buf.columns(0).toSeq.asInstanceOf[Seq[Int]]
      assertEquals(ints, Seq(1, 2, 3))
      val longs = buf.columns(1).toSeq.asInstanceOf[Seq[Long]]
      assertEquals(longs, Seq(100L, 200L, 300L))
    }
  }

  test("CSV with NA marker produces missing values") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """a,b
          |1,hello
          |NA,world
          |3,NA""".stripMargin

      val table = csvStringToHeterogeneousTable(
        "naMarker",
        csv,
        List(
          (0, ColumnTag.I32, None, Some("NA")),
          (1, ColumnTag.StringTag, None, Some("NA"))
        ),
        100
      )

      val buf = table.bufferSegment(0).unsafeRunSync()
      val intCol = buf.columns(0)
      assert(!intCol.isMissing(0))
      assert(intCol.isMissing(1))
      assert(!intCol.isMissing(2))

      val strCol = buf.columns(1)
      assert(!strCol.isMissing(0))
      assert(!strCol.isMissing(1))
      assert(strCol.isMissing(2))
    }
  }

  test("CSV with multiple segments") {
    withTempTaskSystem { implicit tsc =>
      val rows = (1 to 100).map(i => s"$i,${i * 10}").mkString("\n")
      val csv = s"a,b\n$rows"

      val table = csvStringToTable("multiSeg", csv, 2, 25)

      assertEquals(table.numRows, 100L)
      assert(table.columns.head.segments.size == 4)

      // verify all rows present
      val allRows = table.bufferStream.compile.toList
        .unsafeRunSync()
        .flatMap { bt =>
          val col0 = bt.columns(0).toSeq.asInstanceOf[Seq[Int]]
          val col1 = bt.columns(1).toSeq.asInstanceOf[Seq[Int]]
          col0.zip(col1)
        }

      assertEquals(allRows.size, 100)
      assertEquals(allRows.head, (1, 10))
      assertEquals(allRows.last, (100, 1000))
    }
  }

  test("CSV with empty values for doubles") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """x
          |1.5
          |
          |3.5
          |""".stripMargin

      val table = csvStringToDoubleTable("emptyD", csv, 1, 100)
      val buf = table.bufferSegment(0).unsafeRunSync()
      val col = buf.columns(0)
      assert(!col.isMissing(0))
      assert(col.isMissing(1))
      assert(!col.isMissing(2))
      assert(col.isMissing(3))
    }
  }
}

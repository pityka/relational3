package ra3

import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.Duration
import tasks.ExtendedTaskSystemComponents

class ParquetRoundtripSuite extends munit.FunSuite with WithTempTaskSystem {
  override val munitTimeout = Duration(180, "s")

  private def bufferAll(t: Table)(implicit tsc: ExtendedTaskSystemComponents) =
    t.bufferStream.compile.toList.unsafeRunSync()

  test("export then import round-trips all 5 types with nulls") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """i,l,d,s,inst
          |1,100,1.5,alpha,2020-01-01T00:00:00Z
          |,200,,NA,2020-06-15T12:00:00Z
          |3,,3.5,world,2021-01-01T00:00:00Z""".stripMargin

      val table = csvStringToHeterogeneousTable(
        "pq",
        csv,
        List(
          (0, ColumnTag.I32, None, None),
          (1, ColumnTag.I64, None, None),
          (2, ColumnTag.F64, None, None),
          (3, ColumnTag.StringTag, None, Some("NA")),
          (4, ColumnTag.Instant, Some(ra3.InstantParser.ISO), None)
        ),
        100
      )

      val files = table.exportToParquet(snappy = true).unsafeRunSync()
      assertEquals(files.size, 1)

      val reimported =
        ra3.importParquet(files.head, "reimport", 100).unsafeRunSync()

      assertEquals(reimported.numRows, 3L)
      assertEquals(reimported.numCols, 5)
      assertEquals(reimported.colNames.toList, List("i", "l", "d", "s", "inst"))

      val orig = bufferAll(table)
      val round = bufferAll(reimported)
      (0 until 5).foreach { c =>
        val a = orig.flatMap(_.columns(c).toSeq.map(_.toString))
        val b = round.flatMap(_.columns(c).toSeq.map(_.toString))
        assertEquals(b, a, s"column $c mismatch")
      }
    }
  }

  test("export uncompressed round-trips") {
    withTempTaskSystem { implicit tsc =>
      val rows = (1 to 50).map(i => s"$i,${i * 10},val$i").mkString("\n")
      val csv = s"a,b,c\n$rows"
      val table = csvStringToHeterogeneousTable(
        "pq2",
        csv,
        List(
          (0, ColumnTag.I32, None, None),
          (1, ColumnTag.I64, None, None),
          (2, ColumnTag.StringTag, None, None)
        ),
        1000
      )

      val files = table.exportToParquet(snappy = false).unsafeRunSync()
      assertEquals(files.size, 1)

      val reimported =
        ra3.importParquet(files.head, "reimport2", 1000).unsafeRunSync()

      assertEquals(reimported.numRows, 50L)
      val orig = bufferAll(table)
      val round = bufferAll(reimported)
      (0 until 3).foreach { c =>
        val a = orig.flatMap(_.columns(c).toSeq.map(_.toString))
        val b = round.flatMap(_.columns(c).toSeq.map(_.toString))
        assertEquals(b, a, s"column $c mismatch")
      }
    }
  }
}

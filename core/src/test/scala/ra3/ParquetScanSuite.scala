package ra3

import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.Duration
import tasks.ExtendedTaskSystemComponents

class ParquetScanSuite extends munit.FunSuite with WithTempTaskSystem {
  override val munitTimeout = Duration(180, "s")

  private def strCol(t: Table, c: Int)(implicit
      tsc: ExtendedTaskSystemComponents
  ): List[String] =
    t.bufferStream.compile.toList
      .unsafeRunSync()
      .flatMap(_.columns(c).toSeq.map(_.toString))

  test("scanParquet queries a parquet file in place without rewriting") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """i,l,d,s
          |1,100,1.5,alpha
          |2,200,2.5,beta
          |3,300,3.5,gamma
          |4,400,4.5,delta""".stripMargin
      val table = csvStringToHeterogeneousTable(
        "scan",
        csv,
        List(
          (0, ColumnTag.I32, None, None),
          (1, ColumnTag.I64, None, None),
          (2, ColumnTag.F64, None, None),
          (3, ColumnTag.StringTag, None, None)
        ),
        100
      )
      val pf = table.exportToParquet(snappy = true).unsafeRunSync().head

      val scanned = ra3.scanParquet(pf, "scanned").unsafeRunSync()
      assertEquals(scanned.numRows, 4L)
      assertEquals(scanned.colNames.toList, List("i", "l", "d", "s"))

      assertEquals(strCol(scanned, 0), List("1", "2", "3", "4"))
      assertEquals(strCol(scanned, 1), List("100", "200", "300", "400"))
      assertEquals(strCol(scanned, 3), List("alpha", "beta", "gamma", "delta"))

      val hi = scanned
        .as[("i", "l", "d", "s"), (I32Var, I64Var, F64Var, StrVar)]
        .schema(t => query(ra3.all(t).where(t.i > 2)))
        .evaluate
        .unsafeRunSync()
      assertEquals(strCol(hi, 0).toSet, Set("3", "4"))

      val none = scanned
        .as[("i", "l", "d", "s"), (I32Var, I64Var, F64Var, StrVar)]
        .schema(t => query(ra3.all(t).where(t.i > 1000)))
        .evaluate
        .unsafeRunSync()
      assertEquals(none.numRows, 0L)
    }
  }

  test("scanParquet matches importParquet") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """a,b
          |10,x
          |20,y
          |30,z""".stripMargin
      val table = csvStringToHeterogeneousTable(
        "m",
        csv,
        List((0, ColumnTag.I32, None, None), (1, ColumnTag.StringTag, None, None)),
        100
      )
      val pf = table.exportToParquet(snappy = false).unsafeRunSync().head

      val scanned = ra3.scanParquet(pf, "s").unsafeRunSync()
      val imported = ra3.importParquet(pf, "i", 100).unsafeRunSync()

      assertEquals(strCol(scanned, 0), strCol(imported, 0))
      assertEquals(strCol(scanned, 1), strCol(imported, 1))
      assertEquals(strCol(scanned, 0), List("10", "20", "30"))
      assertEquals(strCol(scanned, 1), List("x", "y", "z"))
    }
  }
}

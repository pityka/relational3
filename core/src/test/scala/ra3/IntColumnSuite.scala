package ra3

import cats.effect.unsafe.implicits.global
import ColumnTag.I32

class IntColumnSuite extends munit.FunSuite with WithTempTaskSystem {
  test("filter ==") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 1000
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      println(tableFrame)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)
      val filtered = (for {
        p0 <- ra3Table.columns(0).as(I32).>=(0)
        p1 <- ra3Table.columns(1).as(I32).>=(-1000)
        p2 <- p0.and(p1)
        r <- ra3Table.rfilter(p2)
      } yield r)
        .flatMap(_.bufferStream.compile.toList)
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _).resetRowIndex

      import org.saddle.ops.BinOps._
      val expected = tableFrame.rowAt(
        (tableFrame.colAt(0) > 0 && tableFrame.colAt(1) > -1000)
          .find(identity)
          .toArray
      ).resetRowIndex
      println(tableFrame)
      println(expected)

      assertEquals(filtered, expected)
    }
  }
}

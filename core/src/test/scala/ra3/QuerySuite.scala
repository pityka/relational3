package ra3

import tasks.TaskSystemComponents
import cats.effect.unsafe.implicits.global
import ColumnTag.I32
import ra3.lang._

class QuerySuite extends munit.FunSuite with WithTempTaskSystem {

  def toFrame(t: Table)(implicit tsc: TaskSystemComponents) = {
    t.bufferStream.compile.toList
      .unsafeRunSync()
      .map(_.toHomogeneousFrame(I32))
      .reduce(_ concat _)
      .resetRowIndex
  }

  test("simple filter with pushdown ===") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val result = ra3Table
        .in[DI32] { (table, col0) =>
          table.query(ra3.lang.select(ra3.lang.star).where(col0 === 0))
        }
        .evaluate
        .unsafeRunSync()

      val takenF = (0 until 4)
        .map(i =>
          result.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
        )
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)

      val expect =
        tableFrame.resetRowIndex.rfilter(_.values(0) == 0)

      assertEquals(takenF, expect)
    }

  }
  test("simple filter with pushdown inSet") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val result = ra3Table
        .in[DI32] { (table, col0) =>
          table.query(
            ra3.lang.select(ra3.lang.star).where(col0.containedIn(Set(0, 1)))
          )
        }
        .evaluate
        .unsafeRunSync()

      val takenF = (0 until 4)
        .map(i =>
          result.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
        )
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)

      val expect =
        tableFrame.rfilter(v => Set(0, 1).contains(v.values(0))).resetRowIndex

      assertEquals(takenF, expect)
    }

  }
  test("simple filter with pushdown >=") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val result = ra3Table
        .in[DI32] { (table, col0) =>
            table.query(ra3.lang.select(ra3.lang.star).where(col0 >= 0))
          }.evaluate
        
        .unsafeRunSync()

      val takenF = (0 until 4)
        .map(i =>
          result.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
        )
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)

      val expect =
        tableFrame.rfilter(_.values(0) >= 0).resetRowIndex

      assertEquals(takenF, expect)
    }

  }
  test("simple filter with pushdown <=") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val result = ra3Table
        .in[DI32] { (table, col0) =>
            table.query(ra3.lang.select(ra3.lang.star).where(col0 <= 0))
          
        }.evaluate
        .unsafeRunSync()

      val takenF = (0 until 4)
        .map(i =>
          result.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
        )
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)

      val expect =
        tableFrame.rfilter(_.values(0) <= 0).resetRowIndex

      assertEquals(takenF, expect)
    }

  }

}

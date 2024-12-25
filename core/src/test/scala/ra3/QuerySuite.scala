package ra3

import tasks.TaskSystemComponents
import cats.effect.unsafe.implicits.global
import ColumnTag.I32

class QuerySuite
    extends munit.FunSuite
    with WithTempTaskSystem
    with TableExtensions {

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
      1 to 10 foreach { _ =>
        val (tableFrame, tableCsv) = generateTable(numRows, numCols)
        val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

        val result = ra3Table
          .as[("c0", "c1", "c2"), (ra3.I32Var, ra3.I32Var, ra3.I32Var)]
          .schema({ table =>
            query(ra3.all(table).where(table.c0 === 0))

          })
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
          .setColIndex(org.saddle.Index("c0","c1","c2"))
        assertEquals(takenF, expect)
      }
    }

  }
  test("simple filter with pushdown inSet") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      1 to 10 foreach { _ =>
        val (tableFrame, tableCsv) = generateTable(numRows, numCols)
        val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

        val result = ra3Table
          .as[("c0", "c1", "c2"), (ra3.I32Var, ra3.I32Var, ra3.I32Var)]
          .schema { case table =>
            query(
              ra3.all(table).where(table.c0.containedIn(Set(0, 1)))
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
          .setColIndex(org.saddle.Index("c0","c1","c2"))

        assertEquals(takenF, expect)
      }
    }

  }
  test("simple filter with pushdown >=") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val result = ra3Table
        .as[("c0", "c1", "c2"), (ra3.I32Var, ra3.I32Var, ra3.I32Var)]
        .schema { case table =>
          query(ra3.all(table).where(table.c0 >= 0))
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
        tableFrame.rfilter(_.values(0) >= 0).resetRowIndex
        .setColIndex(org.saddle.Index("c0","c1","c2"))

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
        .as[("c0", "c1", "c2"), (ra3.I32Var, ra3.I32Var, ra3.I32Var)]
        .schema { case table => query(ra3.all(table).where(table.c0 <= 0)) }
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
        tableFrame.rfilter(_.values(0) <= 0).resetRowIndex
        .setColIndex(org.saddle.Index("c0","c1","c2"))

      assertEquals(takenF, expect)
    }

  }
  test("simple * concat") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val prg = ra3Table
        .as[("c0", "c1", "c2"), (ra3.I32Var, ra3.I32Var, ra3.I32Var)]
        .schema { case table => query(ra3.all(table)) }
        .in(t => t.concat(t))

      println(prg.render)
      val result = prg.evaluate
        .unsafeRunSync()

      val takenF = (0 until result.segmentation.size)
        .map(i =>
          result.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
        )
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)

      val expect =
        (tableFrame.concat(tableFrame)).resetRowIndex
        .setColIndex(org.saddle.Index("c0","c1","c2"))

      assertEquals(takenF, expect)
    }

  }

}

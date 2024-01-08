package ra3
import org.saddle._
import java.nio.channels.Channels
import java.io.ByteArrayInputStream
import tasks.TaskSystemComponents
import com.typesafe.config.ConfigFactory
import tasks._
import cats.effect.unsafe.implicits.global
import ColumnTag.I32

class MySuite extends munit.FunSuite {

  def toFrame(t: Table)(implicit tsc: TaskSystemComponents) = {
    t.bufferStream.compile.toList
      .unsafeRunSync()
      .map(_.toHomogeneousFrame(I32))
      .reduce(_ concat _)
      .resetRowIndex
  }

  def withTempTaskSystem[T](f: TaskSystemComponents => T) = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    val config = ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=4
      akka.loglevel=OFF
      tasks.disableRemoting = true
      """
    )
    val r = withTaskSystem(config)(f)
    tmp.delete()
    r
  }

  def generateTable(numRows: Int, numCols: Int) = {
    val frame = mat.randI(numRows, numCols).toFrame.mapColIndex(i => s"V$i")
    val csv = new String(
      org.saddle.csv.CsvWriter.writeFrameToArray(frame, withRowIx = false)
    )
    (frame, csv)
  }

  def csvStringToTable(
      name: String,
      csv: String,
      numCols: Int,
      segmentLength: Int
  )(implicit
      tsc: TaskSystemComponents
  ) = {
    val channel =
      Channels.newChannel(new ByteArrayInputStream(csv.getBytes()))

    ra3.csv
      .readHeterogeneousFromCSVChannel(
        name,
        List(
          0 until (numCols) map (i => (i, ColumnTag.I32)): _*
        ),
        channel = channel,
        header = true,
        maxSegmentLength = segmentLength
      )
      .toOption
      .get
  }

  // test("to and from csv 1 segment") {
  //   withTaskSystem(testConfig) { implicit ts =>
  //     val numCols = 3
  //     val numRows = 10
  //     val (tableFrame,tableCsv) = generateTable(numRows,numCols)
  //     val ra3Table = csvStringToTable("test1",tableCsv,numCols, 1000)
  //     val segment0 = ra3Table.bufferSegment(0).unsafeRunSync()
  //     val f1 = segment0.toHomogeneousFrame(ColumnTag.I32)

  //     assertEquals(f1.filterIx(_.nonEmpty), tableFrame)
  //   }

  // }
  // test("to and from csv more segments") {
  //   withTaskSystem(testConfig) { implicit ts =>
  //     val numCols = 3
  //     val numRows = 10
  //     val (tableFrame, tableCsv) = generateTable(numRows, numCols)
  //     val ra3Table = csvStringToTable("test1", tableCsv, numCols, 3)
  //     assertEquals(ra3Table.columns.map(_.segments.size).distinct.size, 1)
  //     assertEquals(
  //       ra3Table.columns.map(_.segments.map(_.numElems).sum).head,
  //       tableFrame.numRows
  //     )
  //     assertEquals(
  //       ra3Table.columns.map(_.segments.map(_.numElems)).head,
  //       Seq(3, 3, 3, 1)
  //     )
  //     val segment0 = ra3Table.bufferSegment(0).unsafeRunSync()
  //     val segment1 = ra3Table.bufferSegment(1).unsafeRunSync()
  //     val segment2 = ra3Table.bufferSegment(2).unsafeRunSync()
  //     val segment3 = ra3Table.bufferSegment(3).unsafeRunSync()
  //     val f1 = segment0
  //       .toHomogeneousFrame(ColumnTag.I32)
  //       .concat(
  //         segment1.toHomogeneousFrame(ColumnTag.I32)
  //       )
  //       .concat(segment2.toHomogeneousFrame(ColumnTag.I32))
  //       .concat(segment3.toHomogeneousFrame(ColumnTag.I32))
  //       .resetRowIndex

  //     assertEquals(f1.filterIx(_.nonEmpty), tableFrame)
  //   }

  // }
  // test("take") {
  //   withTaskSystem(testConfig) { implicit ts =>
  //     val numCols = 3
  //     val numRows = 10
  //     val (tableFrame, tableCsv) = generateTable(numRows, numCols)
  //     val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)
  //     val idxs = Seq(Seq(0), Seq(), Seq(0), Seq(0, 0))
  //     val indexColumn = I32.makeColumn(
  //       idxs.zipWithIndex
  //         .map(s => (s._2, ColumnTag.I32.makeBufferFromSeq(s._1: _*)))
  //         .map(v =>
  //           v._2.toSegment(LogicalPath("idx1", None, v._1, 0)).unsafeRunSync()
  //         )
  //         .toVector
  //     )

  //     val taken = ra3Table.take(indexColumn).unsafeRunSync()
  //     val takenF = (0 until 4)
  //       .map(i =>
  //         taken.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
  //       )
  //       .reduce(_ concat _)
  //       .resetRowIndex
  //       .filterIx(_.nonEmpty)
  //     val expect =
  //       tableFrame.rowAt(0, 3, 9, 9).resetRowIndex

  //     assertEquals(takenF, expect)
  //   }

  // }
  // test("filter on predicate") {
  //   withTaskSystem(testConfig) { implicit ts =>
  //     val numCols = 3
  //     val numRows = 10
  //     val (tableFrame, tableCsv) = generateTable(numRows, numCols)
  //     val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)
  //     val masks = Seq(Seq(1,0,1), Seq(0,0,0), Seq(1,1,1), Seq(0))
  //     val maskColumn = I32.makeColumn(
  //       masks.zipWithIndex
  //         .map(s => (s._2, ColumnTag.I32.makeBufferFromSeq(s._1: _*)))
  //         .map(v =>
  //           v._2.toSegment(LogicalPath("idx1", None, v._1, 0)).unsafeRunSync()
  //         )
  //         .toVector
  //     )

  //     val taken = ra3Table.rfilter(maskColumn).unsafeRunSync()
  //     val takenF = (0 until 4)
  //       .map(i =>
  //         taken.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
  //       )
  //       .reduce(_ concat _)
  //       .resetRowIndex
  //       .filterIx(_.nonEmpty)
  //     val expect =
  //       tableFrame.rowAt(0,2,6,7,8).resetRowIndex

  //     assertEquals(takenF, expect)
  //   }

  // }
  // test("filter on <>") {
  //   withTempTaskSystem { implicit ts =>
  //     val numCols = 3
  //     val numRows = 10
  //     val (tableFrame, tableCsv) = generateTable(numRows, numCols)
  //     println(tableFrame)
  //     val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)
  //     val literal = I32.makeBufferFromSeq(0).toSegment(LogicalPath("test0",None,0,0)).unsafeRunSync()

  //     val less = ra3Table.rfilterInEquality(0,literal,true).unsafeRunSync()
  //     println(less)
  //     val takenF = (0 until 4)
  //       .map(i =>
  //         less.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
  //       )
  //       .reduce(_ concat _)
  //       .resetRowIndex
  //       .filterIx(_.nonEmpty)
  //     val expect =
  //       tableFrame.rowAt(tableFrame.colAt(0).toVec.find(_ <= 0).toArray).resetRowIndex

  //     assertEquals(takenF, expect)
  //   }

  // }
  // test("partition") {
  //   withTempTaskSystem { implicit ts =>
  //     val numCols = 3
  //     val numRows = 10
  //     val (tableFrame, tableCsv) = generateTable(numRows, numCols)
  //     val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

  //     val partitions = ra3Table.partition(List(0),3).unsafeRunSync()

  //     assert(partitions.size == 3)

  //     partitions.zipWithIndex.foreach{ case (pTable,pIdx) =>
  //       pTable.columns(0).segments.foreach{ segment =>
  //         val b = segment.buffer.unsafeRunSync()
  //         0 until b.length foreach { i =>
  //           val h = math.abs(b.hashOf(i))
  //           assert(h % 3 == pIdx)
  //         }
  //       }
  //     }

  //     val partionedRowsF = partitions.reduce(_ concatenate _).bufferStream.compile.toList.unsafeRunSync().map(_.toHomogeneousFrame(I32)).reduce(_ concat _).resetColIndex.toRowSeq.map(_._2).toSet

  //     val expected = tableFrame.resetColIndex.toRowSeq.map(_._2).toSet

  //     assertEquals(partionedRowsF,expected)

  //   }

  // }
  // test("partition") {
  //   withTempTaskSystem { implicit ts =>
  //     val numCols = 3
  //     val numRows = 10
  //     val (tableFrame, tableCsv) = generateTable(numRows, numCols)
  //     val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

  //     val partitions = ra3Table.partition(List(0), 3).unsafeRunSync()

  //     assert(partitions.size == 3)

  //     partitions.zipWithIndex.foreach { case (pTable, pIdx) =>
  //       pTable.columns(0).segments.foreach { segment =>
  //         val b = segment.buffer.unsafeRunSync()
  //         0 until b.length foreach { i =>
  //           val h = math.abs(b.hashOf(i))
  //           assert(h % 3 == pIdx)
  //         }
  //       }
  //     }

  //     val partionedRowsF = partitions
  //       .reduce(_ concatenate _)
  //       .bufferStream
  //       .compile
  //       .toList
  //       .unsafeRunSync()
  //       .map(_.toHomogeneousFrame(I32))
  //       .reduce(_ concat _)
  //       .resetColIndex
  //       .toRowSeq
  //       .map(_._2)
  //       .toSet

  //     val expected = tableFrame.resetColIndex.toRowSeq.map(_._2).toSet

  //     assertEquals(partionedRowsF, expected)

  //   }

  // }
  // test("outer join") {
  //   withTempTaskSystem { implicit ts =>
  //     val numCols = 3
  //     val numRows = 10
  //     val (tableFrame, tableCsv) = generateTable(numRows, numCols)
  //     val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
  //     val colA = Seq(Seq(0, 0, 1), Seq(0, 2, 3), Seq(4, 5, 0), Seq(9))
  //     val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))

  //     val tF = {
  //       val tmp = tableFrame.addCol(
  //         Series(colA.flatten.toVec),
  //         "V4",
  //         org.saddle.index.InnerJoin
  //       )
  //       tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
  //     }
  //     val tF2 = {
  //       val tmp = tableFrame2
  //         .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
  //         .mapColIndex(v => v + "_2")
  //       tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
  //     }

  //     // println(tF)
  //     // println(tF2)

  //     val saddleResult = tF
  //       .rconcat(
  //         tF2,
  //         org.saddle.index.OuterJoin
  //       )
  //       .filterIx(_ != "V4_2")
  //       .filterIx(_ != "V4")
  //       .col(
  //         "V4",
  //         "V0",
  //         "V1",
  //         "V2",
  //         "V0_2",
  //         "V1_2",
  //         "V2_2"
  //       )
  //       .resetRowIndex

  //     val tableA = csvStringToTable("table", tableCsv, numCols, 3)
  //       .addColumnFromSeq(I32, "V4")(colA.flatten)
  //       .unsafeRunSync()

  //     val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
  //       .addColumnFromSeq(I32, "V4")(colB.flatten)
  //       .unsafeRunSync()
  //       .mapColIndex(_ + "_2")

  //     assertEquals(toFrame(tableA), tF.resetRowIndex)
  //     assertEquals(toFrame(tableB), tF2.resetRowIndex)

  //     val result = tableA
  //       .equijoin(tableB, 3, 3, "outer", 3)
  //       .unsafeRunSync()
  //       .filterColumnNames("joined-filtered")(_ != "V4")
  //       .bufferStream
  //       .compile
  //       .toList
  //       .unsafeRunSync()
  //       .map(_.toHomogeneousFrame(I32))
  //       .reduce(_ concat _)

  //     assertEquals(
  //       saddleResult.toRowSeq.map(_._2).toSet,
  //       result.toRowSeq.map(_._2).toSet
  //     )

  //   }

  // }
  // test("inner join") {
  //   withTempTaskSystem { implicit ts =>
  //     val numCols = 3
  //     val numRows = 10
  //     val (tableFrame, tableCsv) = generateTable(numRows, numCols)
  //     val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
  //     val colA = Seq(Seq(0, 0, 1), Seq(0, 2, 3), Seq(4, 5, 0), Seq(9))
  //     val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))

  //     val tF = {
  //       val tmp = tableFrame.addCol(
  //         Series(colA.flatten.toVec),
  //         "V4",
  //         org.saddle.index.InnerJoin
  //       )
  //       tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
  //     }
  //     val tF2 = {
  //       val tmp = tableFrame2
  //         .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
  //         .mapColIndex(v => v + "_2")
  //       tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
  //     }

  //     // println(tF)
  //     // println(tF2)

  //     val saddleResult = tF
  //       .rconcat(
  //         tF2,
  //         org.saddle.index.InnerJoin
  //       )
  //       .filterIx(_ != "V4_2")
  //       .filterIx(_ != "V4")
  //       .col(
  //         "V4",
  //         "V0",
  //         "V1",
  //         "V2",
  //         "V0_2",
  //         "V1_2",
  //         "V2_2"
  //       )
  //       .resetRowIndex

  //     val tableA = csvStringToTable("table", tableCsv, numCols, 3)
  //       .addColumnFromSeq(I32, "V4")(colA.flatten)
  //       .unsafeRunSync()

  //     val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
  //       .addColumnFromSeq(I32, "V4")(colB.flatten)
  //       .unsafeRunSync()
  //       .mapColIndex(_ + "_2")

  //     assertEquals(toFrame(tableA), tF.resetRowIndex)
  //     assertEquals(toFrame(tableB), tF2.resetRowIndex)

  //     val result = tableA
  //       .equijoin(tableB, 3, 3, "inner", 3)
  //       .unsafeRunSync()
  //       .filterColumnNames("joined-filtered")(_ != "V4")
  //       .bufferStream
  //       .compile
  //       .toList
  //       .unsafeRunSync()
  //       .map(_.toHomogeneousFrame(I32))
  //       .reduce(_ concat _)

  //     assertEquals(
  //       saddleResult.toRowSeq.map(_._2).toSet,
  //       result.toRowSeq.map(_._2).toSet
  //     )

  //   }

  // }
  // test("left outer join") {
  //   withTempTaskSystem { implicit ts =>
  //     val numCols = 3
  //     val numRows = 10
  //     val (tableFrame, tableCsv) = generateTable(numRows, numCols)
  //     val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
  //     val colA = Seq(Seq(0, 0, 1), Seq(0, 2, 3), Seq(4, 5, 0), Seq(9))
  //     val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))

  //     val tF = {
  //       val tmp = tableFrame.addCol(
  //         Series(colA.flatten.toVec),
  //         "V4",
  //         org.saddle.index.InnerJoin
  //       )
  //       tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
  //     }
  //     val tF2 = {
  //       val tmp = tableFrame2
  //         .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
  //         .mapColIndex(v => v + "_2")
  //       tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
  //     }

  //     // println(tF)
  //     // println(tF2)

  //     val saddleResult = tF
  //       .rconcat(
  //         tF2,
  //         org.saddle.index.LeftJoin
  //       )
  //       .filterIx(_ != "V4_2")
  //       .filterIx(_ != "V4")
  //       .col(
  //         "V4",
  //         "V0",
  //         "V1",
  //         "V2",
  //         "V0_2",
  //         "V1_2",
  //         "V2_2"
  //       )
  //       .resetRowIndex

  //     val tableA = csvStringToTable("table", tableCsv, numCols, 3)
  //       .addColumnFromSeq(I32, "V4")(colA.flatten)
  //       .unsafeRunSync()

  //     val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
  //       .addColumnFromSeq(I32, "V4")(colB.flatten)
  //       .unsafeRunSync()
  //       .mapColIndex(_ + "_2")

  //     assertEquals(toFrame(tableA), tF.resetRowIndex)
  //     assertEquals(toFrame(tableB), tF2.resetRowIndex)

  //     val result = tableA
  //       .equijoin(tableB, 3, 3, "left", 3)
  //       .unsafeRunSync()
  //       .filterColumnNames("joined-filtered")(_ != "V4")
  //       .bufferStream
  //       .compile
  //       .toList
  //       .unsafeRunSync()
  //       .map(_.toHomogeneousFrame(I32))
  //       .reduce(_ concat _)

  //     assertEquals(
  //       saddleResult.toRowSeq.map(_._2).toSet,
  //       result.toRowSeq.map(_._2).toSet
  //     )

  //   }

  // }
  // test("right outer join") {
  //   withTempTaskSystem { implicit ts =>
  //     val numCols = 3
  //     val numRows = 10
  //     val (tableFrame, tableCsv) = generateTable(numRows, numCols)
  //     val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
  //     val colA = Seq(Seq(0, 0, 1), Seq(0, 2, 3), Seq(4, 5, 0), Seq(9))
  //     val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))

  //     val tF = {
  //       val tmp = tableFrame.addCol(
  //         Series(colA.flatten.toVec),
  //         "V4",
  //         org.saddle.index.InnerJoin
  //       )
  //       tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
  //     }
  //     val tF2 = {
  //       val tmp = tableFrame2
  //         .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
  //         .mapColIndex(v => v + "_2")
  //       tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
  //     }

  //     // println(tF)
  //     // println(tF2)

  //     val saddleResult = tF
  //       .rconcat(
  //         tF2,
  //         org.saddle.index.RightJoin
  //       )
  //       .filterIx(_ != "V4_2")
  //       .filterIx(_ != "V4")
  //       .col(
  //         "V0",
  //         "V1",
  //         "V2",
  //         "V0_2",
  //         "V1_2",
  //         "V2_2"
  //       )
  //       .resetRowIndex

  //     val tableA = csvStringToTable("table", tableCsv, numCols, 3)
  //       .addColumnFromSeq(I32, "V4")(colA.flatten)
  //       .unsafeRunSync()

  //     val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
  //       .addColumnFromSeq(I32, "V4")(colB.flatten)
  //       .unsafeRunSync()
  //       .mapColIndex(_ + "_2")

  //     assertEquals(toFrame(tableA), tF.resetRowIndex)
  //     assertEquals(toFrame(tableB), tF2.resetRowIndex)

  //     val result = tableA
  //       .equijoin(tableB, 3, 3, "right", 3)
  //       .unsafeRunSync()
  //       .filterColumnNames("joined-filtered")(s => s != "V4" && s != "V4_2")
  //       .bufferStream
  //       .compile
  //       .toList
  //       .unsafeRunSync()
  //       .map(_.toHomogeneousFrame(I32))
  //       .reduce(_ concat _)

  //     assertEquals(
  //       saddleResult.toRowSeq.map(_._2).toSet,
  //       result.toRowSeq.map(_._2).toSet
  //     )

  //   }

  // }
  test("group by") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val colA = Seq(Seq(0, 0, 1), Seq(0, 2, 3), Seq(4, 5, 0), Seq(9))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF.groupBy.groups.map { case (group, idx) =>
        (group, tF.rowAt(idx))
      }.toList

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      assertEquals(toFrame(tableA), tF.resetRowIndex)

      val result = tableA
        .groupBy(List(3), 3)
        .unsafeRunSync()
        .extractGroups
        .unsafeRunSync()
        .map { group =>
          toFrame(group)
        }

      

      assertEquals(saddleResult.map(_._2.resetRowIndex).toSet,result.toSet)

    }

  }
}

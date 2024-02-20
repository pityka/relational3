package ra3
// import ra3._
import org.saddle._
import java.nio.channels.Channels
import java.io.ByteArrayInputStream
import tasks.TaskSystemComponents
import com.typesafe.config.ConfigFactory
import tasks._
import cats.effect.unsafe.implicits.global
import ra3.tablelang._
import ra3.lang._
import ColumnTag.I32
trait WithTempTaskSystem {
  def withTempTaskSystem[T](f: TaskSystemComponents => T) = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    val config = ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=1
      
      akka.loglevel=OFF
      tasks.disableRemoting = true
      """
    )
    val r = withTaskSystem(config)(f)
    tmp.delete()
    r
  }

  def csvStringToHeterogeneousTable(
      name: String,
      csv: String,
      cols: List[(Int, ColumnTag, Option[InstantParser])],
      segmentLength: Int
  )(implicit
      tsc: TaskSystemComponents
  ) = {
    val channel =
      Channels.newChannel(new ByteArrayInputStream(csv.getBytes()))

    val x = ra3.csv
      .readHeterogeneousFromCSVChannel(
        name,
        cols,
        channel = channel,
        header = true,
        maxSegmentLength = segmentLength,
        recordSeparator = "\n"
      )

    x.toOption.get
  }

  def toFrame2(t: Table, tag: ColumnTag)(implicit
      tsc: TaskSystemComponents,
      st: ST[tag.Elem]
  ) = {
    t.bufferStream.compile.toList
      .unsafeRunSync()
      .map(_.toHomogeneousFrame(tag))
      .reduce(_ concat _)
      .resetRowIndex
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
          0 until (numCols) map (i => (i, ColumnTag.I32, None)): _*
        ),
        channel = channel,
        header = true,
        maxSegmentLength = segmentLength
      )
      .toOption
      .get
  }
  def csvStringToStringTable(
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
          0 until (numCols) map (i => (i, ColumnTag.StringTag, None)): _*
        ),
        channel = channel,
        header = true,
        maxSegmentLength = segmentLength
      )
      .toOption
      .get
  }
  def csvStringToDoubleTable(
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
          0 until (numCols) map (i => (i, ColumnTag.F64, None)): _*
        ),
        channel = channel,
        header = true,
        maxSegmentLength = segmentLength
      )
      .toOption
      .get
  }
  def csvStringToLongTable(
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
          0 until (numCols) map (i => (i, ColumnTag.I64, None)): _*
        ),
        channel = channel,
        header = true,
        maxSegmentLength = segmentLength
      )
      .toOption
      .get
  }
}
  


class RelationlAlgebraSuite extends munit.FunSuite with WithTempTaskSystem {

  def toFrame(t: Table)(implicit tsc: TaskSystemComponents) = {
    t.bufferStream.compile.toList
      .unsafeRunSync()
      .map(_.toHomogeneousFrame(I32))
      .reduce(_ concat _)
      .resetRowIndex
  }
  def toLongFrame(t: Table)(implicit tsc: TaskSystemComponents) = {
    t.bufferStream.compile.toList
      .unsafeRunSync()
      .map(_.toHomogeneousFrame(ra3.ColumnTag.I64))
      .reduce(_ concat _)
      .resetRowIndex
  }

  test("to and from csv 1 segment") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("test1", tableCsv, numCols, 1000)
      val segment0 = ra3Table.bufferSegment(0).unsafeRunSync()
      val f1 = segment0.toHomogeneousFrame(ColumnTag.I32)

      assertEquals(f1.filterIx(_.nonEmpty), tableFrame)
    }

  }
  test("to and from csv more segments") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("test1", tableCsv, numCols, 3)
      assertEquals(ra3Table.columns.map(_.segments.size).distinct.size, 1)
      assertEquals(
        ra3Table.columns.map(_.segments.map(_.numElems).sum).head,
        tableFrame.numRows
      )
      assertEquals(
        ra3Table.columns.map(_.segments.map(_.numElems)).head,
        Seq(3, 3, 3, 1)
      )
      val segment0 = ra3Table.bufferSegment(0).unsafeRunSync()
      val segment1 = ra3Table.bufferSegment(1).unsafeRunSync()
      val segment2 = ra3Table.bufferSegment(2).unsafeRunSync()
      val segment3 = ra3Table.bufferSegment(3).unsafeRunSync()
      val f1 = segment0
        .toHomogeneousFrame(ColumnTag.I32)
        .concat(
          segment1.toHomogeneousFrame(ColumnTag.I32)
        )
        .concat(segment2.toHomogeneousFrame(ColumnTag.I32))
        .concat(segment3.toHomogeneousFrame(ColumnTag.I32))
        .resetRowIndex

      assertEquals(f1.filterIx(_.nonEmpty), tableFrame)
    }

  }
  test("take") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)
      val idxs = Seq(Seq(0), Seq(), Seq(0), Seq(0, 0))
      val indexColumn = I32.makeColumn(
        idxs.zipWithIndex
          .map(s => (s._2, ColumnTag.I32.makeBufferFromSeq(s._1: _*)))
          .map(v =>
            v._2.toSegment(LogicalPath("idx1", None, v._1, 0)).unsafeRunSync()
          )
          .toVector
      )

      val taken = ra3Table.take(indexColumn).unsafeRunSync()
      val takenF = (0 until 4)
        .map(i =>
          taken.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
        )
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)
      val expect =
        tableFrame.rowAt(0, 6, 9, 9).resetRowIndex

      assertEquals(takenF, expect)
    }

  }
  test("filter on predicate") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)
      val masks = Seq(Seq(1, 0, 1), Seq(0, 0, 0), Seq(1, 1, 1), Seq(0))
      val maskColumn = I32.makeColumn(
        masks.zipWithIndex
          .map(s => (s._2, ColumnTag.I32.makeBufferFromSeq(s._1: _*)))
          .map(v =>
            v._2.toSegment(LogicalPath("idx1", None, v._1, 0)).unsafeRunSync()
          )
          .toVector
      )

      val taken = ra3Table.rfilter(maskColumn).unsafeRunSync()
      val takenF = (0 until 4)
        .map(i =>
          taken.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
        )
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)
      val expect =
        tableFrame.rowAt(0, 2, 6, 7, 8).resetRowIndex

      assertEquals(takenF, expect)
    }

  }
  test("filter on <>") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      println(tableFrame)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)
      val literal = I32
        .makeBufferFromSeq(0)
        .toSegment(LogicalPath("test0", None, 0, 0))
        .unsafeRunSync()

      val less = ra3Table.rfilterInEquality(0, literal, true).unsafeRunSync()
      println(less)
      val takenF = (0 until 4)
        .map(i => less.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32))
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)
      val expect =
        tableFrame
          .rowAt(tableFrame.colAt(0).toVec.find(_ <= 0).toArray)
          .resetRowIndex

      assertEquals(takenF, expect)
    }

  }
  test("simple projection does not copy segments") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      println(tableFrame)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val result = ra3Table
        .query { table =>
          table.query(ra3.lang.select(ra3.lang.star))
        }.evaluate
        .unsafeRunSync()
      assertEquals(ra3Table.columns.head.segments, result.columns.head.segments)
      val takenF = (0 until 4)
        .map(i =>
          result.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
        )
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)
      val expect =
        tableFrame.resetRowIndex

      assertEquals(takenF, expect)
    }

  }
  test("simple query count") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val less = ra3Table
        .schema[DI32,DI32] { (table,col0, col1) =>
            table.count(ra3.lang
              .select(col1 as "b", col1, ra3.lang.star)
              .where(col0.tap("col0") <= 0))
          
        }.evaluate
        .unsafeRunSync()
      val takenF = (0 until 1)
        .map(i => less.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(ra3.ColumnTag.I64))
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)
        println(takenF)
      val expect =
        Frame("count"->Vec(tableFrame.colAt(0).toVec.countif(_ <= 0).toLong))

      assertEquals(takenF, expect)
    }

  }
  test("simple query") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      println(tableFrame)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val less = ra3Table
        .schema[DI32,DI32] { (table,col0, col1) =>
            table.query(ra3.lang
              .select(col1 as "b", col1, ra3.lang.star)
              .where(col0.tap("col0") <= 0))
          
        }.evaluate
        .unsafeRunSync()
      val takenF = (0 until 4)
        .map(i => less.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32))
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)
      val expect =
        tableFrame
          .rowAt(tableFrame.colAt(0).toVec.find(_ <= 0).toArray)
          .resetRowIndex
          .colAt(Array(1, 1, 0, 1, 2))
          .setColIndex(Index("b", "V1", "V0", "V1", "V2"))

      assertEquals(takenF, expect)
    }

  }
  test("simple query tablelang") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      println(tableFrame)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      import ra3.lang._
      import cats.effect.unsafe.implicits.global
      val less2 = schema[DI32,DI32](ra3Table) { (t1,v0,v1) =>
          t1.query(
            ra3.lang
              .select(v1 as "b", v1.unnamed, ra3.lang.star)
              .where(v0.tap("col0") <= 0)
          )
      }.evaluate.unsafeRunSync()

      val takenF2 = (0 until 4)
        .map(i =>
          less2.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32)
        )
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)
      val expect =
        tableFrame
          .rowAt(tableFrame.colAt(0).toVec.find(_ <= 0).toArray)
          .resetRowIndex
          .colAt(Array(1, 1, 0, 1, 2))
          .setColIndex(Index("b", "V1", "V0", "V1", "V2"))

      println(takenF2)
      println(expect)
      // assertEquals(takenF, expect)
      assertEquals(takenF2, expect)
    }

  }
  test("partition") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val partitions = ra3Table.partition(List(0), 3, true, 0,2).unsafeRunSync()

      assert(partitions.size == 3)

      partitions.zipWithIndex.foreach { case (pTable, pIdx) =>
        pTable.columns(0).segments.foreach { segment =>
          val b = segment.buffer.unsafeRunSync()
          0 until b.length foreach { i =>
            val h = math.abs(b.hashOf(i))
            assert(h % 3 == pIdx)
          }
        }
      }

      val partionedRowsF = partitions
        .reduce(_ concatenate _)
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)
        .resetColIndex
        .toRowSeq
        .map(_._2)
        .toSet

      val expected = tableFrame.resetColIndex.toRowSeq.map(_._2).toSet

      assertEquals(partionedRowsF, expected)

    }

  }
  test("partition") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val partitions = ra3Table.partition(List(0), 3, true, 0,2).unsafeRunSync()

      assert(partitions.size == 3)

      partitions.zipWithIndex.foreach { case (pTable, pIdx) =>
        pTable.columns(0).segments.foreach { segment =>
          val b = segment.buffer.unsafeRunSync()
          0 until b.length foreach { i =>
            val h = math.abs(b.hashOf(i))
            assert(h % 3 == pIdx)
          }
        }
      }

      val partionedRowsF = partitions
        .reduce(_ concatenate _)
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)
        .resetColIndex
        .toRowSeq
        .map(_._2)
        .toSet

      val expected = tableFrame.resetColIndex.toRowSeq.map(_._2).toSet

      assertEquals(partionedRowsF, expected)

    }

  }
  test("outer join") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val colA = Seq(Seq(0, 0, 1), Seq(0, 2, 3), Seq(4, 5, 0), Seq(9))
      val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.OuterJoin
        )
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4")
        .col(
          "V4",
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2"
        )
        .resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

        //     partitionBase: Int,
  //     partitionLimit: Int,
  //     maxSegmentsToBufferAtOnce: Int

      val result = tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumn[DI32](3){ aCol3 =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.outer(bCol3).withPartitionBase(3).withPartitionLimit(0).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(ra3.lang.star))
              }
            }  
        }

      }.evaluate
       
        .unsafeRunSync()
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("inner join with filter pushdown") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows*2, numCols)
      val colA = Seq(Seq(1, 1, 2), Seq(1, 1, 1), Seq(2, 2, 1), Seq(1))
      val colB = Seq(Seq(3,3,4), Seq(1, 2, 1), Seq(2, 1, 2), Seq(1),Seq(3,3,4), Seq(1, 2, 1), Seq(2, 1, 2), Seq(1))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.InnerJoin
        )
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4")
        .col(
          "V0",
          "V4",
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2"
        )
        .resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val result = 
        tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.inner(bCol3).withPartitionBase(3).withPartitionLimit(11).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(aCol0,ra3.lang.star))
              }
            }  
        }

      }.evaluate

       
        .unsafeRunSync()
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      println(result)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("inner join with 3") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val colA = Seq(Seq(1, 1, 2), Seq(1, 1, 1), Seq(2, 2, 1), Seq(1))
      val colB = Seq(Seq(3,3,4), Seq(1, 2, 1), Seq(2, 1, 2), Seq(1))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.InnerJoin
        )
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4")
        .col(
          "V0",
          "V4",
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2"
        )
        .resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val result = 
         tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.inner(bCol3).withPartitionBase(3).withPartitionLimit(0).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(aCol0,ra3.lang.star))
              }
            }  
        }

      }.evaluate
        
       
        .unsafeRunSync()
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("inner join 2") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val colA = Seq(Seq(1, 1, 2), Seq(1, 1, 1), Seq(2, 2, 1), Seq(1))
      val colB = Seq(Seq(2, 2, 1), Seq(1, 2, 1), Seq(2, 1, 2), Seq(1))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.InnerJoin
        )
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4")
        .col(
          "V4",
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2"
        )
        .resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val result =  tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.inner(bCol3).withPartitionBase(3).withPartitionLimit(0).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(ra3.lang.star))
              }
            }  
        }

      }.evaluate
        .unsafeRunSync()
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("inner join") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val colA = Seq(Seq(0, 0, 1), Seq(0, 2, 3), Seq(4, 5, 0), Seq(9))
      val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.InnerJoin
        )
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4")
        .col(
          "V4",
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2"
        )
        .resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val result = tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.inner(bCol3).withPartitionBase(3).withPartitionLimit(0).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(ra3.lang.star))
              }
            }  
        }

      }.evaluate
        .unsafeRunSync()
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("inner join 3") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val (tableFrame3, tableCsv3) = generateTable(numRows + 1, numCols)
      val colA = Seq(Seq(0, 0, 1), Seq(0, 2, 3), Seq(4, 5, 0), Seq(9))
      val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))
      val colC = Seq(Seq(3, 3, 3), Seq(4, 4, 4), Seq(99, 0, 1), Seq(5, 99))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }
      val tF3 = {
        val tmp = tableFrame3
          .addCol(Series(colC.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_3")
        tmp.setRowIndex(Index(tmp.firstCol("V4_3").toVec.toArray))
      }

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.InnerJoin
        )
        .rconcat(tF3, org.saddle.index.InnerJoin)
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4_3")
        .filterIx(_ != "V4")
        .col(
          "V4",
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2",
          "V0_3",
          "V1_3",
          "V2_3"
        )
        .resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")
      val tableC = csvStringToTable("tableC", tableCsv3, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colC.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_3")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)
      assertEquals(toFrame(tableC), tF3.resetRowIndex)

      val result = 
        tableA.query{ tableA =>
        tableB.query{ tableB =>
        tableC.query{ tableC =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
              tableC.useColumn[DI32](3) {cCol3 =>
                aCol3.join.inner(bCol3).inner(cCol3).withPartitionBase(3).withPartitionLimit(10).elementwise(ra3.lang.select(ra3.lang.star))
              }
            }  }
        }}

      }.evaluate

        
        .unsafeRunSync()
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .filterColumnNames("joined-filtered")(_ != "V4_3")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("inner join filter") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val colA = Seq(Seq(0, 0, 1), Seq(0, 2, 3), Seq(4, 5, 0), Seq(9))
      val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.InnerJoin
        )
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4")
        .col(
          "V4",
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2"
        )
        .resetRowIndex
        .rfilter(_.first("V0").get <= 0)

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val result = tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.inner(bCol3).withPartitionBase(3).withPartitionLimit(0).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(ra3.lang.star).where(aCol0 <= 0))
              }
            }  
        }

      }.evaluate
        .unsafeRunSync()
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("left outer join") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val colA = Seq(Seq(0, 0, 1), Seq(0, 2, 3), Seq(4, 5, 0), Seq(9))
      val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.LeftJoin
        )
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4")
        .col(
          "V4",
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2"
        )
        .resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val result = tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.left(bCol3).withPartitionBase(3).withPartitionLimit(0).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(ra3.lang.star))
              }
            }  
        }

      }.evaluate
        .unsafeRunSync()
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("right outer join") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val colA = Seq(Seq(0, 0, 1), Seq(0, 2, 3), Seq(4, 5, 0), Seq(9))
      val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.RightJoin
        )
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4")
        .col(
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2"
        )
        .resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val result = tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.right(bCol3).withPartitionBase(3).withPartitionLimit(0).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(ra3.lang.star))
              }
            }  
        }

      }.evaluate
        .unsafeRunSync()
        .filterColumnNames("joined-filtered")(s => s != "V4" && s != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
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
        .groupBy(List(3), 3, 0,2)
        .unsafeRunSync()
        .extractGroups
        .unsafeRunSync()
        .map { group =>
          toFrame(group)
        }

      assertEquals(saddleResult.map(_._2.resetRowIndex).toSet, result.toSet)

    }

  }
  test("reduce by") {

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

      val saddleResult = tF.groupBy.combine(_.sum).resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      assertEquals(toFrame(tableA), tF.resetRowIndex)


      val result = toFrame(
        tableA.schema[DI32,DI32,DI32,DI32]( (_,c1,c2,c3,c4) =>
            c4.groupBy.withPartitionBase(3).withPartitionLimit(0).withMaxSegmentsBufferingAtOnce(2)
            .apply(
              ra3.lang.select(c1.sum, c2.sum, c3.sum, c4.sum as "V4")
            ).all
          ).evaluate
          
          .unsafeRunSync()
      )


      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("count groups") {

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

      val saddleResult = tF.groupBy.combine(_.sum).resetRowIndex.colAt(0).toVec.countif(_ <= 0)

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      assertEquals(toFrame(tableA), tF.resetRowIndex)


      val result = toLongFrame(
        tableA.schema[DI32,DI32,DI32,DI32]( (_,c1,c2,c3,c4) =>
            c4.groupBy.withPartitionBase(3).withPartitionLimit(0).withMaxSegmentsBufferingAtOnce(2)
            .apply(
              ra3.lang.select(c1.sum, c2.sum, c3.sum, c4.sum as "V4")
              .where(c1.sum <= 0)
            ).count
          ).evaluate
          
          .unsafeRunSync()
      )


      assertEquals(
        Frame("count" -> Vec(saddleResult.toLong)),
        result
      )

    }

  }
  test("reduce complete table") {

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

      val saddleResult = Frame(tF.reduce(_.sum)).T

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      assertEquals(toFrame(tableA), tF.resetRowIndex)

      println(tableA)

      val result = toFrame(
        tableA.schema[DI32,DI32,DI32,DI32]( (table,c1,c2,c3,c4) =>
          table.reduce(
                    ra3.lang.select(c1.sum, c2.sum, c3.sum, c4.sum as "V4"))
            
        ).evaluate
          .unsafeRunSync()
      )

      println(saddleResult)
      println(result)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("top k") {

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

      val saddleResult = tF.sortedRows(0).head(2)
      val saddleResultDesc = tF.sortedRows(0).tail(2)

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      assertEquals(toFrame(tableA), tF.resetRowIndex)

      val result = toFrame(
        tableA
          .topK(0, true, 3, 1.0, 5)
          .unsafeRunSync()
      )
      val resultDesc = toFrame(
        tableA
          .topK(0, false, 3, 1.0, 5)
          .unsafeRunSync()
      )

      println(tF.sortedRows(0))
      println(result)
      assert(result.numRows <= 5)
      assert(saddleResultDesc.numRows <= 5)
      assert(
        (saddleResult.toRowSeq.map(_._2).toSet &~ result.toRowSeq
          .map(_._2)
          .toSet).isEmpty
      )
      assert(
        (saddleResultDesc.toRowSeq.map(_._2).toSet &~ resultDesc.toRowSeq
          .map(_._2)
          .toSet).isEmpty
      )

    }

  }
  test("filter on <>") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      println(tableFrame)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)
      val literal = I32
        .makeBufferFromSeq(Int.MaxValue)
        .toSegment(LogicalPath("test0", None, 0, 0))
        .unsafeRunSync()

      val less = ra3Table.rfilterInEquality(0, literal, false).unsafeRunSync()
      assert(less.columns.head.segments.forall(s => s.as(I32).sf.isEmpty))
      val takenF = (0 until 4)
        .map(i => less.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32))
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)
      val expect =
        tableFrame
          .rowAt(tableFrame.colAt(0).toVec.find(_ >= Int.MaxValue).toArray)
          .resetRowIndex

      assertEquals(takenF, expect)
    }

  }

  test("inner join with no overlap") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val colA = Seq(Seq(0, 0, 1), Seq(100, 101, 102), Seq(4, 5, 0), Seq(9))
      val colB =
        Seq(Seq(200, 202, 201), Seq(200, 201, 202), Seq(204, 205, 200), Seq(4))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.InnerJoin
        )
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4")
        .col(
          "V4",
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2"
        )
        .resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val joined = tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.inner(bCol3).withPartitionBase(3).withPartitionLimit(0).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(ra3.lang.star))
              }
            }  
        }

      }.evaluate
        .unsafeRunSync()

      println(joined.columns.head.segments.mkString("\n"))

      val result = joined
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("inner join with no partitioning on one side ") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(5, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val colA = Seq(Seq(0, 0, 1), Seq(0, 2))
      val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.InnerJoin
        )
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4")
        .col(
          "V4",
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2"
        )
        .resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val joined = tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.inner(bCol3).withPartitionBase(3).withPartitionLimit(6).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(ra3.lang.star))
              }
            }  
        }

      }.evaluate
        .unsafeRunSync()

      val result = joined
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      val joined2 = tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.inner(bCol3).withPartitionBase(3).withPartitionLimit(6).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(ra3.lang.star))
              }
            }  
        }

      }.evaluate
        .unsafeRunSync()

      val result2 = joined2
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)
        .col("V0", "V1", "V2", "V0_2", "V1_2", "V2_2")

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )
      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result2.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("inner join with prepartition on compatible partitioning on one side ") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(5, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val colA = Seq(Seq(0, 0, 1), Seq(0, 2))
      val colB = Seq(Seq(2, 2, 1), Seq(99, 2, 0), Seq(4, 5, 0), Seq(4))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }
      val tF2 = {
        val tmp = tableFrame2
          .addCol(Series(colB.flatten.toVec), "V4", org.saddle.index.InnerJoin)
          .mapColIndex(v => v + "_2")
        tmp.setRowIndex(Index(tmp.firstCol("V4_2").toVec.toArray))
      }

      // println(tF)
      // println(tF2)

      val saddleResult = tF
        .rconcat(
          tF2,
          org.saddle.index.InnerJoin
        )
        .filterIx(_ != "V4_2")
        .filterIx(_ != "V4")
        .col(
          "V4",
          "V0",
          "V1",
          "V2",
          "V0_2",
          "V1_2",
          "V2_2"
        )
        .resetRowIndex

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val pre = tableB.prePartition(List(3, 0), 3, 0,2).unsafeRunSync()
      println(pre)

      val joined = tableA.query{ tableA =>
        pre.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.inner(bCol3).withPartitionBase(3).withPartitionLimit(6).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(ra3.lang.star))
              }
            }  
        }

      }.evaluate
        .unsafeRunSync()

      val result = joined
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      val joined2 = tableA.query{ tableA =>
        tableB.query{ tableB =>
          tableA.useColumns[DI32,DI32](0,3){ (aCol0,aCol3) =>
              tableB.useColumn[DI32](3) {bCol3 =>
                aCol3.join.inner(bCol3).withPartitionBase(3).withPartitionLimit(6).withMaxSegmentsBufferingAtOnce(2).elementwise(ra3.lang.select(ra3.lang.star))
              }
            }  
        }

      }.evaluate
        .unsafeRunSync()

      val result2 = joined2
        .filterColumnNames("joined-filtered")(_ != "V4")
        .filterColumnNames("joined-filtered")(_ != "V4_2")
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)
        .col("V0", "V1", "V2", "V0_2", "V1_2", "V2_2")


      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )
      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result2.toRowSeq.map(_._2).toSet
      )

    }

  }

}

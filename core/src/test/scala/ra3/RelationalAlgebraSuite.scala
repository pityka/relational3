package ra3
// import ra3.*
import org.saddle.{ST, mat, Frame, Series, Vec, SeqToVec, Index}
import java.nio.channels.Channels
import java.io.ByteArrayInputStream
import tasks.TaskSystemComponents
import org.ekrich.config.ConfigFactory
import tasks.*
import cats.effect.unsafe.implicits.global
import ColumnTag.I32
trait WithTempTaskSystem extends TableExtensions {
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
    val r = withTaskSystem(config)(tsc => cats.effect.IO(f)).unsafeRunSync()
    tmp.delete()
    r
  }

  def csvStringToHeterogeneousTable(
      name: String,
      csv: String,
      cols: List[(Int, ColumnTag, Option[InstantParser], Option[String])],
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
          0 until (numCols) map (i => (i, ColumnTag.I32, None, None))*
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
          0 until (numCols) map (i => (i, ColumnTag.StringTag, None, None))*
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
          0 until (numCols) map (i => (i, ColumnTag.F64, None, None))*
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
          0 until (numCols) map (i => (i, ColumnTag.I64, None, None))*
        ),
        channel = channel,
        header = true,
        maxSegmentLength = segmentLength
      )
      .toOption
      .get
  }

}

class RelationlAlgebraSuite
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
  def toLongFrame(t: Table)(implicit tsc: TaskSystemComponents) = {
    t.bufferStream.compile.toList
      .unsafeRunSync()
      .map(_.toHomogeneousFrame(ra3.ColumnTag.I64))
      .reduce(_ concat _)
      .resetRowIndex
  }

  test("dictionary segment") {
    withTempTaskSystem { implicit ts =>
      val strings = 1 to 10000 map (_ => scala.util.Random.alphanumeric.take(20).mkString) toArray
    val b = BufferString(strings*)
    val readBack = b
      .toSegment(LogicalPath("test", None, 0, 0))
      .flatMap(segment => segment.buffer)
      .unsafeRunSync()
    assert(readBack.values.toVector.map(_.toString) == strings.toVector)

    }
  }
  test("length prefix segment") {
    withTempTaskSystem { implicit ts =>
      val strings = 1 to 300000 map (_ => scala.util.Random.alphanumeric.take(20).mkString) toArray
    val b = BufferString(strings*)
    val readBack = b
      .toSegment(LogicalPath("test", None, 0, 0))
      .flatMap(segment =>  {assert(segment.sf.get.isInstanceOf[ra3.segmentstring.PrefixLength]);segment.buffer})
      .unsafeRunSync()
    assert(readBack.values.toVector.map(_.toString) == strings.toVector)

    }
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
          .map(s => (s._2, ColumnTag.I32.makeBufferFromSeq(s._1*)))
          .map(v =>
            ColumnTag.I32
              .toSegment(v._2, LogicalPath("idx1", None, v._1, 0))
              .unsafeRunSync()
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

  test("filter on <>") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      println(tableFrame)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)
      val literal = I32
        .toSegment(
          I32
            .makeBufferFromSeq(0),
          LogicalPath("test0", None, 0, 0)
        )
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
        .as[("c0", "c1", "c2"), (I32Var, I32Var, I32Var)]
        .schema { t =>
          ra3.query(ra3.all(t))
        }
        .evaluate
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
          .setColIndex(Index("c0", "c1", "c2"))

      assertEquals(takenF, expect)
    }

  }
  test("simple query count") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)
        .as[("c0", "c1"), (I32Var, I32Var)]

      val less = ra3Table
        .schema { table =>
          ra3
            .count(
              ((ra3.S :* (table.c0.as("b")) :* table.c1.as("b1"))
              // .extend(t)
              )
            )
            .where(table.c0.tap("col0") <= 0)

        }
        .evaluate
        .unsafeRunSync()
      val takenF = (0 until 1)
        .map(i =>
          less
            .bufferSegment(i)
            .unsafeRunSync()
            .toHomogeneousFrame(ra3.ColumnTag.I64)
        )
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)
      println(takenF)
      val expect =
        Frame("count" -> Vec(tableFrame.colAt(0).toVec.countif(_ <= 0).toLong))

      assertEquals(takenF, expect)
    }

  }
  test("simple query") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val less = ra3Table
        .as[("c0", "c1", "c2"), (I32Var, I32Var, I32Var)]
        .schema { case table =>
          query(
            (((table.c0 as "b") :* table.c1.as("b1"))
              .concat(ra3.all(table)))
              .where(table.c0.tap("col0") <= 0)
          )

        }
        .evaluate
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
          .colAt(Array(0, 1, 0, 1, 2))
          .setColIndex(Index("b", "b1", "c0", "c1", "c2"))

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

      import cats.effect.unsafe.implicits.global
      val less2 = ra3Table
        .as[("c0", "c1", "c2"), (I32Var, I32Var, I32Var)]
        .schema { t =>
          query(
            (((t.c1 as "b") :* t.c1.as("b1"))
              .concat(ra3.all(t)))
              .where(t.c0.tap("col0") <= 0)
          )

        }
        .evaluate
        .unsafeRunSync()

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
          .setColIndex(Index("b", "b1", "c0", "c1", "c2"))

      println(takenF2)
      println(expect)
      // assertEquals(takenF, expect)
      assertEquals(takenF2, expect)
    }

  }
  test("partition query") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("table", tableCsv, numCols, 3)

      val partitions = ra3Table
        .as[("c0", "c1", "c2"), (I32Var, I32Var, I32Var)]
        .schema { t =>
          t.c0.partitionBy
            .withPartitionBase(3)
            .withPartitionLimit(0)
            .withMaxSegmentsBufferingAtOnce(2)
            .done
        }
        .evaluate
        .unsafeRunSync()

    assert(partitions.partitions.isDefined)
    assert(
      partitions.partitions.get.partitionMapOverSegments.distinct.sorted == Vector(
        0,
        1,
        2
      )
    )

    (0 until 3).foreach { case (pIdx) =>
      partitions.partitions.get.partitionMapOverSegments.zipWithIndex
        .filter(_._1 == pIdx)
        .map(_._2)
        .foreach { sIdx =>
          val pTable = partitions.bufferSegment(sIdx).unsafeRunSync()
          val columnBuffer = pTable.columns(0)
          0 until columnBuffer.length foreach { i =>
            val h = math.abs(columnBuffer.hashOf(i))
            assert(h % 3 == pIdx)

          }
        }
    }

    val partionedRowsF = partitions.bufferStream.compile.toList
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

      val partitions =
        ra3Table.partition(List(0), 3, true, 0, 2).unsafeRunSync()

      assert(partitions.size == 3)

      partitions.zipWithIndex.foreach { case (pTable, pIdx) =>
        val column = pTable.columns(0)
        column.segments.foreach { segment =>
          val b = column.tag.buffer(segment).unsafeRunSync()
          0 until b.length foreach { i =>
            val h = math.abs(b.hashOf(i))
            assert(h % 3 == pIdx)
          }
        }
      }

      val partionedRowsF = partitions
        .reduce(_ `concatenate` _)
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

      val partitions =
        ra3Table.partition(List(0), 3, true, 0, 2).unsafeRunSync()

      assert(partitions.size == 3)

      partitions.zipWithIndex.foreach { case (pTable, pIdx) =>
        val column = pTable.columns(0)
        column.segments.foreach { segment =>
          val b = column.tag.buffer(segment).unsafeRunSync()
          0 until b.length foreach { i =>
            val h = math.abs(b.hashOf(i))
            assert(h % 3 == pIdx)
          }
        }
      }

      val partionedRowsF = partitions
        .reduce(_ `concatenate` _)
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
        .resetRowIndex
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3"
          )
        )

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
      //     maxItemsToBufferAtOnce: Int

      val result = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .outer(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(0)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()
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
      val (tableFrame2, tableCsv2) = generateTable(numRows * 2, numCols)
      val colA = Seq(Seq(1, 1, 2), Seq(1, 1, 1), Seq(2, 2, 1), Seq(1))
      val colB = Seq(
        Seq(3, 3, 4),
        Seq(1, 2, 1),
        Seq(2, 1, 2),
        Seq(1),
        Seq(3, 3, 4),
        Seq(1, 2, 1),
        Seq(2, 1, 2),
        Seq(1)
      )

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
        .resetRowIndex
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3"
          )
        )

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      // val result =
      //   tableA
      //     .in { tableA =>
      //       tableB.in { tableB =>
      //         tableA[I32Var, I32Var](0, 3) { (aCol0, aCol3) =>
      //           tableB.apply[I32Var](3) { bCol3 =>
      //             aCol3.join(ra3.select(aCol0, ra3.star))
      //               .inner(bCol3)
      //               .withPartitionBase(3)
      //               .withPartitionLimit(11)
      //               .withMaxSegmentsBufferingAtOnce(2)
      //               .done
      //           }
      //         }
      //       }

      //     }
      val result = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .inner(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(0)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()
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
  test("inner join with 3") {
    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val (tableFrame2, tableCsv2) = generateTable(numRows, numCols)
      val colA = Seq(Seq(1, 1, 2), Seq(1, 1, 1), Seq(2, 2, 1), Seq(1))
      val colB = Seq(Seq(3, 3, 4), Seq(1, 2, 1), Seq(2, 1, 2), Seq(1))

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
        .resetRowIndex
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3"
          )
        )

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val result = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .inner(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(0)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()
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
        .resetRowIndex
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3"
          )
        )

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val result = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .inner(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(0)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()
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
        .resetRowIndex
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3"
          )
        )

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      // val result = tableA
      //   .in { tableA =>
      //     tableB.in { tableB =>
      //       tableA[I32Var, I32Var](0, 3) { (_, aCol3) =>
      //         tableB.apply[I32Var](3) { bCol3 =>
      //           aCol3.join(ra3.select(ra3.star))
      //             .inner(bCol3)
      //             .withPartitionBase(3)
      //             .withPartitionLimit(0)
      //             .withMaxSegmentsBufferingAtOnce(2)
      //             .done
      //         }
      //       }
      //     }

      //   }
      val result = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .inner(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(0)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()
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
        .resetRowIndex
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3",
            "c_c0",
            "c_c1",
            "c_c2",
            "c_c3"
          )
        )

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

      // val result =
      //   tableA
      //     .in { tableA =>
      //       tableB.in { tableB =>
      //         tableC.in { tableC =>
      //           tableA[I32Var, I32Var](0, 3) { (_, aCol3) =>
      //             tableB.apply[I32Var](3) { bCol3 =>
      //               tableC.apply[I32Var](3) { cCol3 =>
      //                 aCol3.join(ra3.select(ra3.star))
      //                   .inner(bCol3)
      //                   .inner(cCol3)
      //                   .withPartitionBase(3)
      //                   .withPartitionLimit(10)
      //                   .done
      //               }
      //             }
      //           }
      //         }
      //       }

      //     }
      val result = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableC
                .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
                .schema { tableC =>
                  tableA.c3
                    .inner(tableB.c3)
                    .inner(tableC.c3)
                    .withPartitionBase(3)
                    .withPartitionLimit(10)
                    .withMaxSegmentsBufferingAtOnce(2)
                    .select(
                      ra3
                        .all(tableA)
                        .concat(ra3.allWithPrefix(tableB, "b_"))
                        .concat(ra3.allWithPrefix(tableC, "c_"))
                    )

                }
            }
        }
        .evaluate
        .unsafeRunSync()
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
        .resetRowIndex
        .rfilter(_.first("V0").get <= 0)
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3"
          )
        )

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      // val result = tableA
      //   .in { tableA =>
      //     tableB.in { tableB =>
      //       tableA[I32Var, I32Var](0, 3) { (aCol0, aCol3) =>
      //         tableB.apply[I32Var](3) { bCol3 =>
      //           aCol3.join(ra3.select(ra3.star).where(aCol0 <= 0))
      //             .inner(bCol3)
      //             .withPartitionBase(3)
      //             .withPartitionLimit(0)
      //             .withMaxSegmentsBufferingAtOnce(2)
      //             .done
      //         }
      //       }
      //     }

      //   }
      val result = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .inner(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(0)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))
                .where(tableA.c0 <= 0)

            }

        }
        .evaluate
        .unsafeRunSync()
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
        .resetRowIndex
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3"
          )
        )

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val result = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .left(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(0)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()
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
        .resetRowIndex
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3"
          )
        )

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val result = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .right(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(0)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()
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
        .groupBy(List(3), 3, 0, 2)
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

      val saddleResult = {
        import org.saddle.*
        tF.groupBy.combine(_.sum).resetRowIndex
      }

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      assertEquals(toFrame(tableA), tF.resetRowIndex)

      val result = toFrame(
        tableA
          .as[("c1", "c2", "c3", "c4"), (I32Var, I32Var, I32Var, I32Var)]
          .schema { tableA =>
            tableA.c4.groupBy
              .withPartitionBase(3)
              .withPartitionLimit(0)
              .withMaxSegmentsBufferingAtOnce(2)
              .reduceTotal(
                ((tableA.c1.sum.as("V0") :* tableA.c2.sum.as(
                  "V1"
                ) :* tableA.c3.sum.as("V2") :* (tableA.c4.sum as "V4")))
              )
          }
          .evaluate
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

      val saddleResult = {
        import org.saddle.*
        tF.groupBy.combine(_.sum).resetRowIndex.colAt(0).toVec.countif(_ <= 0)
      }

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      assertEquals(toFrame(tableA), tF.resetRowIndex)

      val result = toLongFrame(
        tableA
          .as[("c1", "c2", "c3", "c4"), (I32Var, I32Var, I32Var, I32Var)]
          .schema { t =>
            t.c4.groupBy
              .withPartitionBase(3)
              .withPartitionLimit(0)
              .withMaxSegmentsBufferingAtOnce(2)
              .count(
                ((t.c1.sum.as("V0") :* t.c2.sum.as("V1") :* t.c3.sum
                  .as("V2") :* (t.c4.sum as "V3")))
              )
              .where(t.c1.sum <= 0)
          }
          .evaluate
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

      val saddleResult = {
        import org.saddle.*
        Frame(tF.reduce(_.sum)).T
      }

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      assertEquals(toFrame(tableA), tF.resetRowIndex)

      println(tableA)

      val result = toFrame(
        tableA
          .as[("c1", "c2", "c3", "c4"), (I32Var, I32Var, I32Var, I32Var)]
          .schema { t =>
            reduce(
              (t.c1.sum.as("V0") :* t.c2.sum.as("V1") :* t.c3.sum
                .as("V2") :* (t.c4.sum as "V4"))
            )
          }
          .evaluate
          .unsafeRunSync()
      )

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

    }

  }
  test("top k with missing") {

    withTempTaskSystem { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val colA =
        Seq(Seq(0, 0, 1), Seq(Int.MinValue, 2, 3), Seq(4, 5, 0), Seq(9))

      val tF = {
        val tmp = tableFrame.addCol(
          Series(colA.flatten.toVec),
          "V4",
          org.saddle.index.InnerJoin
        )
        tmp.setRowIndex(Index(tmp.firstCol("V4").toVec.toArray))
      }

      val saddleResult = tF.sortedRows(3).rfilter(_.at(3).isDefined).head(2)
      val saddleResultDesc = tF.sortedRows(3).rfilter(_.at(3).isDefined).tail(2)

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      assertEquals(toFrame(tableA), tF.resetRowIndex)

      val result = toFrame(
        tableA
          .topK(3, true, 3, 1.0, 5)
          .unsafeRunSync()
      )
      val resultDesc = toFrame(
        tableA
          .topK(3, false, 3, 1.0, 5)
          .unsafeRunSync()
      )

      assert(result.numRows <= 5)
      assert(saddleResultDesc.numRows <= 5)
      assert(result.colAt(3).toVec.toSeq.filter(_ == 0).size == 3)
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
  test("top k, expr") {

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
          .as[("c0", "c1", "c2"), (I32Var, I32Var, I32Var)]
          .schema { case t => t.c0.topK(true, 3, 1d, 5) }
          .evaluate
          .unsafeRunSync()
      )
      val resultDesc = toFrame(
        tableA
          .as[("c0", "c1", "c2"), (I32Var, I32Var, I32Var)]
          .schema { case t => t.c0.topK(false, 3, 1d, 5) }
          .evaluate
          .unsafeRunSync()
      )

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
        .toSegment(
          I32
            .makeBufferFromSeq(Int.MaxValue),
          LogicalPath("test0", None, 0, 0)
        )
        .unsafeRunSync()

      val less = ra3Table.rfilterInEquality(0, literal, false).unsafeRunSync()
      assert(
        less.columns.head.segments.forall(s =>
          s.asInstanceOf[SegmentInt].sf.isEmpty
        )
      )
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
        .resetRowIndex
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3"
          )
        )

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      // val joined = tableA
      //   .in { tableA =>
      //     tableB.in { tableB =>
      //       tableA[I32Var, I32Var](0, 3) { (_, aCol3) =>
      //         tableB.apply[I32Var](3) { bCol3 =>
      //           aCol3.join(ra3.select(ra3.star))
      //             .inner(bCol3)
      //             .withPartitionBase(3)
      //             .withPartitionLimit(0)
      //             .withMaxSegmentsBufferingAtOnce(2)
      //             .done

      //         }
      //       }
      //     }

      //   }
      val joined = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .inner(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(0)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()

      println(joined.columns.head.segments.mkString("\n"))

      val result = joined.bufferStream.compile.toList
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
        .resetRowIndex
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3"
          )
        )
      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      // val joined = tableA
      //   .in { tableA =>
      //     tableB.in { tableB =>
      //       tableA[I32Var, I32Var](0, 3) { (_, aCol3) =>
      //         tableB.apply[I32Var](3) { bCol3 =>
      //           aCol3.join(ra3.select(ra3.star))
      //             .inner(bCol3)
      //             .withPartitionBase(3)
      //             .withPartitionLimit(6)
      //             .withMaxSegmentsBufferingAtOnce(2)
      //             .done

      //         }
      //       }
      //     }

      //   }
      val joined = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .inner(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(6)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()

      val result = joined.bufferStream.compile.toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      // val joined2 = tableA
      //   .in { tableA =>
      //     tableB.in { tableB =>
      //       tableA[I32Var, I32Var](0, 3) { (_, aCol3) =>
      //         tableB.apply[I32Var](3) { bCol3 =>
      //           aCol3.join(ra3.select(ra3.star))
      //             .inner(bCol3)
      //             .withPartitionBase(3)
      //             .withPartitionLimit(6)
      //             .withMaxSegmentsBufferingAtOnce(2)
      //             .done

      //         }
      //       }
      //     }

      //   }
      val joined2 = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .inner(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(6)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()

      val result2 = joined2.bufferStream.compile.toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

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
        .resetRowIndex
        .setColIndex(
          Index(
            "c0",
            "c1",
            "c2",
            "c3",
            "b_c0",
            "b_c1",
            "b_c2",
            "b_c3"
          )
        )

      val tableA = csvStringToTable("table", tableCsv, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colA.flatten)
        .unsafeRunSync()

      val tableB = csvStringToTable("tableB", tableCsv2, numCols, 3)
        .addColumnFromSeq(I32, "V4")(colB.flatten)
        .unsafeRunSync()
        .mapColIndex(_ + "_2")

      assertEquals(toFrame(tableA), tF.resetRowIndex)
      assertEquals(toFrame(tableB), tF2.resetRowIndex)

      val pre = tableB.prePartition(List(3, 0), 3, 0, 2).unsafeRunSync()
      println(pre)

      val joined = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          pre
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .inner(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(6)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()

      val result = joined.bufferStream.compile.toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result.toRowSeq.map(_._2).toSet
      )

      val joined2 = tableA
        .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
        .schema { tableA =>
          tableB
            .as[("c0", "c1", "c2", "c3"), (I32Var, I32Var, I32Var, I32Var)]
            .schema { tableB =>
              tableA.c3
                .inner(tableB.c3)
                .withPartitionBase(3)
                .withPartitionLimit(6)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(ra3.all(tableA).concat(ra3.allWithPrefix(tableB, "b_")))

            }

        }
        .evaluate
        .unsafeRunSync()

      val result2 = joined2.bufferStream.compile.toList
        .unsafeRunSync()
        .map(_.toHomogeneousFrame(I32))
        .reduce(_ concat _)

      assertEquals(
        saddleResult.toRowSeq.map(_._2).toSet,
        result2.toRowSeq.map(_._2).toSet
      )

    }

  }

}

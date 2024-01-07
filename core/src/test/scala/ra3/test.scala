package ra3
import org.saddle._
import java.nio.channels.Channels
import java.io.ByteArrayInputStream
import tasks.TaskSystemComponents
import com.typesafe.config.ConfigFactory
import tasks._
import cats.effect.unsafe.implicits.global

class MySuite extends munit.FunSuite {

  def testConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=4
      akka.loglevel=OFF
      tasks.disableRemoting = true
      """
    )
  }

  def generateTable(numRows: Int, numCols: Int) = {
    val frame = mat.randI(numRows, numCols).toFrame.mapColIndex(i => s"V$i")
    val csv = new String(org.saddle.csv.CsvWriter.writeFrameToArray(frame))
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
          0 until (numCols + 1) map (i => (i, ColumnTag.I32)): _*
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
  test("to and from csv more segments") {
    withTaskSystem(testConfig) { implicit ts =>
      val numCols = 3
      val numRows = 10
      val (tableFrame, tableCsv) = generateTable(numRows, numCols)
      val ra3Table = csvStringToTable("test1", tableCsv, numCols, 3)
      println(ra3Table)
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
}

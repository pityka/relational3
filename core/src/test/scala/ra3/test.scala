package ra3
import org.saddle._
import java.nio.channels.Channels
import java.io.ByteArrayInputStream
import tasks.TaskSystemComponents
import com.typesafe.config.ConfigFactory
import tasks._

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

  def csvStringToTable(name: String, csv: String, numCols: Int)(implicit
      tsc: TaskSystemComponents
  ) = {
    val channel =
      Channels.newChannel(new ByteArrayInputStream(csv.getBytes()))

    ra3.csv
      .readHeterogeneousFromCSVChannel(
        name,
        List(
          0 until numCols map (i => (i, ColumnTag.I32)): _*
        ),
        channel = channel,
        recordSeparator = "\n",
        header = true
      )
      .toOption
      .get
  }

  test("hello") {
    withTaskSystem(testConfig) { implicit ts =>
      val obtained = 42
      val expected = 43
      assertEquals(obtained, expected)
    }

  }
}

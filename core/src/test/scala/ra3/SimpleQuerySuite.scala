package ra3
import org.saddle._
import java.nio.channels.Channels
import java.io.ByteArrayInputStream
import tasks.TaskSystemComponents
import com.typesafe.config.ConfigFactory
import tasks._
import cats.effect.unsafe.implicits.global
import ColumnTag.I32
import ra3.lang._
class SimpleQuerySuite extends munit.FunSuite with WithTempTaskSystem {

  test("simple query op containedIn Set[Int]") {
    withTempTaskSystem { implicit ts =>
      val (tableFrame, tableCsv) = {
        val frame = Frame(
          Vec(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
          Vec(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
        )
        val csv = new String(
          org.saddle.csv.CsvWriter.writeFrameToArray(frame, withRowIx = false)
        )
        (frame, csv)
      }
      println(tableFrame)
      val ra3Table = csvStringToTable("table", tableCsv, 2, 3)

      val less = ra3Table
        .schema[DI32,DI32] { (tab,col0, col1) =>
            tab.query(ra3.lang
              .select(ra3.lang.star)
              .where(col0.tap("col0").containedIn(Set(1, 2))))
          
        }.evaluate
        .unsafeRunSync()
      println(less)
      val takenF = (0 until 4)
        .map(i => less.bufferSegment(i).unsafeRunSync().toHomogeneousFrame(I32))
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)

      val expect =
        tableFrame
          .rowAt(tableFrame.colAt(0).toVec.find(i => Set(1,2).contains(i)).toArray)
          .resetRowIndex
          .setColIndex(Index("0", "1"))

      assertEquals(takenF, expect)
    }

  }
  test("simple query op containedIn Set[String]") {
    withTempTaskSystem { implicit ts =>
      val (tableFrame, tableCsv) = {
        val frame = Frame(
          Vec(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
          Vec(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
        )
        val csv = new String(
          org.saddle.csv.CsvWriter.writeFrameToArray(frame, withRowIx = false)
        )
        (frame, csv)
      }
      println(tableFrame)
      val ra3Table = csvStringToStringTable("table", tableCsv, 2, 3)
      import ra3.lang.{global => _,_}
      val less =
        ra3.lang.schema[DStr,DStr](ra3Table){ case (t,col0,col1) =>
          t.query(ra3.lang
              .select(ra3.lang.star)
              .where(col0.tap("col0").containedIn(Set("1", "2"))))
          }.evaluate
        .unsafeRunSync()
      println(less)
      val takenF = (0 until 4)
        .map(i => less.bufferSegment(i).unsafeRunSync().toStringFrame)
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)

      val expect =
        tableFrame
          .rowAt(tableFrame.colAt(0).toVec.find(i => Set(1,2).contains(i)).toArray)
          .resetRowIndex
          .setColIndex(Index("0", "1")).mapValues(_.toString)

      assertEquals(takenF, expect)
    }

  }
}

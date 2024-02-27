import cats.effect.unsafe.implicits.global
import java.nio.channels.Channels
import ra3._

class OneBrcSuite extends munit.FunSuite with WithTempTaskSystem {

  test("1brc group by and reduce") {
    withTempTaskSystem { implicit tsc =>
      val channel =
        Channels.newChannel(getClass().getResourceAsStream("/1brc.1M.txt"))
      val channel2 =
        Channels.newChannel(getClass().getResourceAsStream("/1brc.1M.txt"))

      val table = ra3.csv
        .readHeterogeneousFromCSVChannel(
          "1brc",
          List(
            (0, ColumnTag.StringTag, None, None),
            (1, ColumnTag.F64, None, None)
            // 0 until (numCols) map (i => (i, ColumnTag.I32, None)): _*
          ),
          channel = channel,
          header = false,
          maxSegmentLength = 10000,
          fieldSeparator = ';',
          recordSeparator = "\n"
        )
        .toOption
        .get
        .mapColIndex {
          case "V0" => "station"
          case "V1" => "value"
        }

      val result = let[StrVar, F64Var](table) { case (station, value) =>
        station
          .groupBy(
            select(
              station.first as "station",
              value.sum as "sum",
              value.count as "count"
            )
          )
          .partial
          .in[StrVar, F64Var, F64Var] { (station, sum, count) =>
            station
              .groupBy(
                select(
                  station.first,
                  sum.sum / count.sum
                )
              )
              .all

          }
      }.evaluate
        .unsafeRunSync()
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toStringFrame)
        .reduce(_ concat _)
        .withRowIndex(0)
        .colAt(0)
        .mapValues(_.toDouble)
        .sorted

      println(result)

      val expected = org.saddle.csv.CsvParser
        .parseFromChannel[String](
          channel2,
          fieldSeparator = ';',
          recordSeparator = "\n"
        )
        .toOption
        .get
        ._1
        .withRowIndex(0)
        .colAt(0)
        .mapValues(_.toDouble)
        .groupBy
        .combine(_.mean)
        .sorted

      println(expected)
      // .toOption
      // .get
      println(table)
      import org.saddle.ops.BinOps._
      assert((result - expected).values.toSeq.forall(v => math.abs(v) < 1e-3))
      assert(table.numRows == 1000000)
      assertEquals(result.index.sorted, expected.index.sorted)
    }
  }
  test("1brc filter") {
    withTempTaskSystem { implicit tsc =>
      val channel =
        Channels.newChannel(getClass().getResourceAsStream("/1brc.1M.txt"))
      val channel2 =
        Channels.newChannel(getClass().getResourceAsStream("/1brc.1M.txt"))

      val table = ra3.csv
        .readHeterogeneousFromCSVChannel(
          "1brc",
          List(
            (0, ColumnTag.StringTag, None, None),
            (1, ColumnTag.F64, None, None)
            // 0 until (numCols) map (i => (i, ColumnTag.I32, None)): _*
          ),
          channel = channel,
          header = false,
          maxSegmentLength = 10000,
          fieldSeparator = ';',
          recordSeparator = "\n"
        )
        .toOption
        .get
        .mapColIndex {
          case "V0" => "station"
          case "V1" => "value"
        }

      val result = let[StrVar, F64Var](table) { case (station, _) =>
        query(select(star).where(station === "Skopje"))

      }.evaluate
        .unsafeRunSync()
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toStringFrame)
        .reduce(_ concat _)
        .withRowIndex(0)
        .colAt(0)
        .mapValues(_.toDouble)
        .sorted
        .mapVec(_.roundTo(2))

      println(result)

      val expected = org.saddle.csv.CsvParser
        .parseFromChannel[String](
          channel2,
          fieldSeparator = ';',
          recordSeparator = "\n"
        )
        .toOption
        .get
        ._1
        .withRowIndex(0)
        .colAt(0)
        .mapValues(_.toDouble)
        .filterIx(_ == "Skopje")
        .sorted
        .mapVec(_.roundTo(2))

      println(expected)
      // .toOption
      // .get
      println(table)
      assert(table.numRows == 1000000)
      assert(result == expected)
    }
  }
  test("unique stations") {
    withTempTaskSystem { implicit tsc =>
      val channel =
        Channels.newChannel(getClass().getResourceAsStream("/1brc.1M.txt"))
      val channel2 =
        Channels.newChannel(getClass().getResourceAsStream("/1brc.1M.txt"))

      val table = ra3.csv
        .readHeterogeneousFromCSVChannel(
          "1brc",
          List(
            (0, ColumnTag.StringTag, None, None),
            (1, ColumnTag.F64, None, None)
            // 0 until (numCols) map (i => (i, ColumnTag.I32, None)): _*
          ),
          channel = channel,
          header = false,
          maxSegmentLength = 10000,
          fieldSeparator = ';',
          recordSeparator = "\n"
        )
        .toOption
        .get
        .mapColIndex {
          case "V0" => "station"
          case "V1" => "value"
        }

      val result = let[StrVar, F64Var](table) { case (station, _) =>
        station
          .groupBy(
            select(
              station.first as "station"
            )
          )
          .all
          .in[StrVar, F64Var, F64Var] { (station, _, _) =>
            station
              .groupBy(
                select(
                  station.first
                )
              )
              .all

          }

      }.evaluate
        .unsafeRunSync()
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toStringFrame)
        .reduce(_ concat _)
        .colAt(0)
        .toVec
        .toSeq
        .sorted

      val expected = org.saddle.csv.CsvParser
        .parseFromChannel[String](
          channel2,
          fieldSeparator = ';',
          recordSeparator = "\n"
        )
        .toOption
        .get
        ._1
        .withRowIndex(0)
        .colAt(0)
        .mapValues(_.toDouble)
        .index
        .toSeq
        .distinct
        .sorted

      assert(table.numRows == 1000000)
      assert(result == expected)
    }
  }

}

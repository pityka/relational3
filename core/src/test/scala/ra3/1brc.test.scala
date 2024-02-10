import cats.effect.unsafe.implicits.global
import java.nio.channels.Channels
import ra3.lang._
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
            (0, ColumnTag.StringTag, None),
            (1, ColumnTag.F64, None)
            // 0 until (numCols) map (i => (i, ColumnTag.I32, None)): _*
          ),
          channel = channel,
          header = false,
          maxSegmentLength = 100000,
          fieldSeparator = ';',
          recordSeparator = "\n"
        )
        .toOption
        .get
        .mapColIndex {
          case "V0" => "station"
          case "V1" => "value"
        }

      val result = schema[DStr, DF64](table) { case (_, station, value) =>
        station.groupBy.partial.reduceGroupsWith(
            select(
              station.first as "station",
              value.sum as "sum",
              value.count as "count"
            )
          )
          .in[DStr, DF64, DF64] { (_, station, sum, count) =>
            station.groupBy.reduceGroupsWith(
              select(
                station.first,
                sum.sum / count.sum
              )
            )

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
        .groupBy
        .combine(_.mean)
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
            (0, ColumnTag.StringTag, None),
            (1, ColumnTag.F64, None)
            // 0 until (numCols) map (i => (i, ColumnTag.I32, None)): _*
          ),
          channel = channel,
          header = false,
          maxSegmentLength = 100000,
          fieldSeparator = ';',
          recordSeparator = "\n"
        )
        .toOption
        .get
        .mapColIndex {
          case "V0" => "station"
          case "V1" => "value"
        }

      val result = schema[DStr, DF64](table) { case (table, station, _) =>
        table.query(select(star).where(station === "Skopje"))
          
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
        .filterIx(_ == "Skopje").sorted
        .mapVec(_.roundTo(2))

      println(expected)
      // .toOption
      // .get
      println(table)
      assert(table.numRows == 1000000)
      assert(result == expected)
    }
  }
  

}

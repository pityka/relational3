import cats.effect.unsafe.implicits.global
import java.nio.channels.Channels
import ra3.*
import org.saddle.doubleOrd
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
            // 0 until (numCols) map (i => (i, ColumnTag.I32, None))*
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

      val tpe = table.schema[StrVar, F64Var]

      val result = tpe.columnsTuple
        .all { case (station, value) =>
          val t0 = station
            .groupBy(
              ra3.select0
                .extend(station.first as "station")
                .extend(value.sum as "sum")
                .extend(value.count as "count")
            )
            .partial

          val t = t0
            .columnsTuple { schema =>
              schema.columns { case (station, sum, count) =>
                station
                  .groupBy(
                    schema.none
                      .extend(station.first)
                      .extend((sum.sum / count.sum).unnamed)

                      // / count.sum

                  )
                  .all
              }
            }

          t
        }
        .evaluate
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
      import org.saddle.ops.BinOps.*
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
            // 0 until (numCols) map (i => (i, ColumnTag.I32, None))*
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

      val tpe = table.schema[StrVar, F64Var]

      val result = tpe
        .columnsTuple(schema =>
          schema.columns { (station, _) =>
            query(schema.all.where(station === "Skopje"))

          }
        )
        .evaluate
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
            // 0 until (numCols) map (i => (i, ColumnTag.I32, None))*
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
      val typedTable = table.schema[StrVar, F64Var]

      val result = typedTable
        .columnsTuple(schema =>
          schema.columns { (station, col1) =>
            station
              .groupBy(
                select0.extend(
                  station.first as "station"
                )
              )
              .all
              .columnsTuple
              .all { case (station *: EmptyTuple) =>
                station
                  .groupBy(
                    select0.extend(
                      station.first
                    )
                  )
                  .all

              }

          }
        )
        .evaluate
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

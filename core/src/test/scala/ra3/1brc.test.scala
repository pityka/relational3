package ra3
import cats.effect.unsafe.implicits.global
import java.nio.channels.Channels
import org.saddle.doubleOrd
import org.saddle.index.InnerJoin
import java.nio.charset.CharsetDecoder
class OneBrcSuite extends munit.FunSuite with WithTempTaskSystem {

  test("1brc export") {
    withTempTaskSystem { implicit tsc =>
      val channel =
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
          charset = java.nio.charset.Charset.forName("UTF-8").newDecoder(),
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

      val files = table
        .exportToCsv(
          compression = None,
          recordSeparator = "\n",
          columnSeparator = ';'
        )
        .flatMap(v =>
          cats.effect.IO
            .parSequenceN(1)(v.map(_.bytes.map(_.decodeUtf8Lenient)))
        )
        .unsafeRunSync()
      val cat = files.mkString
      val exp = scala.io.Source
        .fromInputStream(getClass().getResourceAsStream("/1brc.1M.txt"))
        .mkString
      assert(
        cat == exp
      )
    }
  }
  test("1brc partial reduce") {
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

      val tpe = table.as[(StrVar, F64Var)]

      val result = tpe
        .schema { (station, value) => _ =>
          ra3.partialReduce(ra3.S :* value.max)
        }
        .in(_.schema { case (max *: EmptyTuple) =>
          _ => ra3.reduce(ra3.S :* max.max)

        })
        .evaluate
        .unsafeRunSync()
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toStringFrame())
        .reduce(_ concat _)
        .colAt(0)
        .mapValues(_.toDouble)
        .sorted
        .toVec
        .raw(0)

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
        .max
        .get

      println(expected)
      // .toOption
      // .get
      println(table)
      import org.saddle.ops.BinOps.*
      assertEquals(result, expected)
    }
  }
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

      val tpe = table.as[(StrVar, F64Var)]

      val result = tpe
        .schema { case (station, value) =>
          _ =>
            val t0 = station.groupBy.reducePartial(
              ra3.select0
                .extend(station.first as "station")
                .extend(value.sum as "sum")
                .extend(value.count as "count")
            )

            val t = t0
              .schema { case (station, sum, count) =>
                schema =>
                  station.groupBy.reduceTotal(
                    schema.none
                      .extend(station.first)
                      .extend((sum.sum / count.sum).unnamed)
                  )
              }

            t
        }
        .evaluate
        .unsafeRunSync()
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toStringFrame())
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

      val tpe = table.as[(StrVar, F64Var)]

      val result = tpe
        .schema({ (station, _) => schema =>
          query(schema.all.where(station === "Skopje"))

        })
        .evaluate
        .unsafeRunSync()
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toStringFrame())
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
      val typedTable = table.as[(StrVar, F64Var)]

      val result = typedTable
        .schema { (station, col1) => _ =>
          station.groupBy
            .reduceTotal(
              select0.extend(
                station.first as "station"
              )
            )
            .schema { case (station *: EmptyTuple) =>
              _ =>
                station.groupBy.reduceTotal(
                  select0.extend(
                    station.first
                  )
                )

            }

        }
        .evaluate
        .unsafeRunSync()
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toStringFrame())
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

  test("1brc equals min or max value") {
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
        .as[(StrVar, F64Var)]

      val program = for {
        stations <- table.tap("tap")
        maxValues <- stations.schema { case (station, value) =>
          _ =>
            station.groupBy
              .reduceTotal(
                station.first :* value.max
              )
        }
        minValues <- stations.schema { case (station, value) =>
          _ =>
            station.groupBy
              .reduceTotal(
                station.first :* value.min
              )
        }
        minMax <- stations.schema { case (station, value) =>
          _ =>
            maxValues.schema { case (maxStation, maxValue) =>
              _ =>
                minValues.schema { case minValues =>
                  _ =>
                    station
                      .inner(maxStation)
                      .inner(minValues._1)
                      .select(
                        (station :* value :* maxValue :* minValues._2 :* (
                          (value === maxValue).ifelse("HIGHEST", "LOWEST")
                        ))
                      )
                      .where(
                        (value === maxValue) || (value === minValues._2)
                      )
                }
            }
        }
      } yield minMax
      println(program.render)

      val f = program.evaluate
        .unsafeRunSync()
        .bufferStream
        .compile
        .toList
        .unsafeRunSync()
        .map(_.toStringFrame())
        .reduce(_ concat _)
      println(f)
      val data =
        org.saddle.csv.CsvParser
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
      import org.saddle._
      val min = Frame("min" -> data.groupBy.combine(_.min.get))
      val max = Frame("max" -> data.groupBy.combine(_.max.get))

      val joined = Frame(
        min
          .rconcat(max, InnerJoin)
          .rconcat(Frame("value" -> data), InnerJoin)
          .rfilter(r =>
            r.get("value") == r.get("min") || r.get("value") == r.get("max")
          )
          .toRowSeq
          .map { case (ix, row) =>
            row
              .mapValues(_.toString)
              .concat(
                Series(
                  "station" -> ix,
                  "minmax" -> (if (row.get("value") == row.get("min")) "LOWEST"
                               else "HIGHEST")
                )
              )
          }*
      ).T
        .col("station", "value", "max", "min", "minmax")
        .setColIndex(Index("V0", "V1", "V2", "V3", "V4"))

      assert(f.toRowSeq.toSet.map(_._2) == joined.toRowSeq.toSet.map(_._2))

    }
  }

}

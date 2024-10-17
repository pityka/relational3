package ra3

import scala.util.Random
import java.io.File
import tasks._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.Duration

class QuerySuite2 extends munit.FunSuite with WithTempTaskSystem {
  override val munitTimeout = Duration(180, "s")
  test("test1") {

    def groupBy(
        customer: ra3.lang.Expr.DelayedIdent[StrVar],
        value: ra3.lang.Expr.DelayedIdent[F64Var]
    ) = {
      customer.groupBy
        .reducePartial(
          customer.first.unnamed :*
            value.sum.unnamed :*
            value.count.unnamed :*
            value.min.unnamed :*
            value.max.unnamed
        )
        .schema { case (customer, sum, count, min, max) =>
          _ =>
            customer.groupBy
              .reduceTotal(
                (customer.first `as` "customer") :*
                  ((sum.sum / count.sum) `as` "avg") :*
                  (min.min `as` "min") :*
                  (max.max `as` "max")
              )
        }
    }

    val path = fixture()
    withTempTaskSystem { implicit tsc =>
      val prg = for {
        table <- parseTransactions(path)
        queryPrg <- IO.pure {
          for {
            byCustomerIn <- table.schema {
              (customerIn, customerOut, _, _, value, _) => _ =>
                groupBy(customerIn, value)
            }
            byCustomerOut <- table.schema {
              (_, customerOut, _, _, value, _) => _ =>
                groupBy(customerOut, value)
            }
            result <- byCustomerOut.schema {
              (customerOut, avgOutValue, _, _) => _ =>
                byCustomerIn.schema { (customerIn, avgInValue, _, _) => _ =>
                  customerIn
                    .outer(customerOut)
                    .select(
                      (customerIn as "cIn")
                        :* (customerOut as "cOut")
                        :* (
                          customerIn.isMissing
                            .ifelse(
                              customerOut,
                              customerIn
                            ) as "customer"
                        )
                        :* (avgInValue as "inAvg")
                        :* (avgOutValue as "outAvg")
                    )
                }
            }
          } yield result
        }

        _ <- IO { println(queryPrg.render) }

        eval <- queryPrg.evaluateToStream

      } yield eval

      val stream = prg.unsafeRunSync()
      val set = stream.compile.toList
        .unsafeRunSync()
        .toSet
        .map(v => (v._3.toString, f"${v._4}%.2f", f"${v._5}%.2f"))

      val f = org.saddle.csv.CsvParser
        .parseFile[String](path, recordSeparator = "\n", fieldSeparator = '\t')
        .toOption
        .get
      import org.saddle._
      val byIn = f
        .col(0, 4)
        .withRowIndex(0)
        .colAt(0)
        .groupBy
        .combine(_.map(_.toDouble).mean)
      val byOut = f
        .col(1, 4)
        .withRowIndex(0)
        .colAt(0)
        .groupBy
        .combine(_.map(_.toDouble).mean)
      val joined = Frame(byIn, byOut).toRowSeq.map { case (ix, row) =>
        (
          ix,
          f"${row.at(0).getOrElse(Double.NaN)}%.2f",
          f"${row.at(1).getOrElse(Double.NaN)}%.2f"
        )
      }.toSet

      assert(joined == set)
    }
  }

  val fixture = new Fixture[File]("file") {
    val path = java.io.File.createTempFile("querysuite", ".tsv")
    runGenerate(300000, path.getAbsolutePath())

    def apply() = path

    override def afterAll(): Unit = {
      path.delete
    }
  }

  def parseTransactions(
      file: File
  )(implicit tsc: TaskSystemComponents) =
    for {
      fileHandle <- SharedFile(uri =
        tasks.util.Uri(
          s"file://${file.getAbsolutePath()}"
        )
      )
      table <- ra3
        .importCsv(
          file = fileHandle,
          name = fileHandle.name,
          columns = (
            ra3.CSVColumnDefinition.StrColumn(0),
            ra3.CSVColumnDefinition.StrColumn(1),
            ra3.CSVColumnDefinition.StrColumn(2),
            ra3.CSVColumnDefinition.I32Column(3),
            ra3.CSVColumnDefinition.F64Column(4),
            ra3.CSVColumnDefinition.InstantColumn(5)
          ),
          maxSegmentLength = 1_000_000,
          recordSeparator = "\n",
          fieldSeparator = '\t',
          compression = None
        )
      renamed = table.table.mapColIndex {
        case "V0" => "customer1"
        case "V1" => "customer2"
        case "V2" => "category_string"
        case "V3" => "category_int"
        case "V4" => "value"
        case "V5" => "instant"
      }
      _ <- IO {
        println(renamed)
      }
      sample <- renamed.showSample(nrows = 10)
      _ <- IO {
        println(sample)
      }
    } yield table

  /** Generate bogus data Each row is a transaction of some value between two
    * customers Each row consists of:
    *   - an id with cardinality in the millions, e.g. some customer id1
    *   - an id with cardinality in the millions, e.g. some customer id2
    *   - an id with cardinality in the hundred thousands e.g. some category id
    *   - an id with cardinality in the thousands e.g. some category id
    *   - a float value, e.g. a price
    *   - an instant
    *
    * The file is block gzipped
    */
  def runGenerate(size: Long, path: String) = {

    def makeRow() = {
      val millions = Random.alphanumeric.take(4).mkString
      val millions2 = Random.alphanumeric.take(4).mkString
      val hundredsOfThousands1 = Random.alphanumeric.take(3).mkString
      val thousands = Random.nextInt(5000)
      val float = Random.nextDouble()
      val instant = java.time.Instant
        .now()
        .plus(
          Random.nextLong(1000 * 60 * 60 * 24L),
          java.time.temporal.ChronoUnit.MILLIS
        )

      (s"$millions\t$millions2\t$hundredsOfThousands1\t$thousands\t$float\t$instant\n")
    }

    Iterator
      .continually {
        makeRow()

      }
      .grouped(100_000)
      .take((size / 100_000).toInt)
      .zipWithIndex
      .foreach { case (group, idx) =>
        val fos1 = (
          new java.io.FileOutputStream(path, true)
        )
        group.foreach { line =>
          fos1.write(line.getBytes("US-ASCII"))
        }
        println((idx + 1) * 100000)
        fos1.close
      }

  }

}

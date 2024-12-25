package ra3

import scala.util.Random
import java.io.File
import tasks._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.Duration

class QuerySuite3 extends munit.FunSuite with WithTempTaskSystem {
  override val munitTimeout = Duration(180, "s")
  test("test1") {

    def groupBy(
        customer: ra3.lang.Expr.DelayedIdent[StrVar],
        value: ra3.lang.Expr.DelayedIdent[F64Var]
    ) = {
      customer.groupBy
        .reducePartial(
          customer.first.as("customer") :*
            value.sum.as("sum") :*
            value.count.as("count") :*
            value.min.as("min") :*
            value.max.as("max")
        )
        .schema { case t =>
          t.customer.groupBy
            .reduceTotal(
              (t.customer.first `as` "customer") :*
                ((t.sum.sum / t.count.sum) `as` "avg") :*
                (t.min.min `as` "min") :*
                (t.max.max `as` "max")
            )
        }
    }

    val path = fixture()
    withTempTaskSystem { implicit tsc =>
      val prg = for {
        table <- parseTransactions(path)
        queryPrg <- IO.pure {
          for {
            byCustomerIn <- table.schema { t =>
              groupBy(t.customerIn, t.value)
            }
            byCustomerOut <- table.schema { t =>
              groupBy(t.customerOut, t.value)
            }
            result <-
              byCustomerIn.customer
                .outer(byCustomerOut.customer)
                .select(
                  ra3.S
                    :* (
                      byCustomerIn.customer.isMissing
                        .ifelse(
                          byCustomerOut.customer,
                          byCustomerIn.customer
                        ) `as` "customer"
                    )
                )

          } yield result
        }

        _ <- IO { println(queryPrg.render) }

        eval <- queryPrg.evaluateToStreamOfSingleColumn.map(_.flatten)

      } yield eval

      val stream = prg.unsafeRunSync()
      val set = stream.compile.toList
        .unsafeRunSync()
        .toSet
        .map(_.toString)

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
      val joined = Frame(byIn, byOut).toRowSeq
        .map { case (ix, row) =>
          (
            ix,
            f"${row.at(0).getOrElse(Double.NaN)}%.2f",
            f"${row.at(1).getOrElse(Double.NaN)}%.2f"
          )
        }
        .toSet
        .map(_._1)

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

    } yield ra3
      .importCsv(
        file = fileHandle,
        name = fileHandle.name,
        columns = (
          customerIn = ra3.CSVColumnDefinition.StrColumn(0),
          customerOut = ra3.CSVColumnDefinition.StrColumn(1),
          cat = ra3.CSVColumnDefinition.StrColumn(2),
          catId = ra3.CSVColumnDefinition.I32Column(3),
          value = ra3.CSVColumnDefinition.F64Column(4),
          time = ra3.CSVColumnDefinition.InstantColumn(5)
        ),
        maxSegmentLength = 1_000_000,
        recordSeparator = "\n",
        fieldSeparator = '\t',
        compression = None
      )

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

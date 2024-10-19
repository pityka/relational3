import tasks.*
import cats.effect.unsafe.implicits.global
import cats.effect.IO
import com.typesafe.config.ConfigFactory
import scala.util.Random
import mainargs.{main, arg, ParserForMethods, Flag}
import ra3.{StrVar, I32Var, F64Var, select, TableExpr, Table}
import ra3.lang.Expr.DelayedIdent
import java.time.temporal.TemporalUnit
import ra3.lang.ReturnValueTuple

object Main {

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
        val fos1 = new java.util.zip.GZIPOutputStream(
          new java.io.FileOutputStream(path, true)
        )
        group.foreach { line =>
          fos1.write(line.getBytes("US-ASCII"))
        }
        println((idx + 1) * 100000)
        fos1.close
      }

  }

  def runQuery(path: String) = {

    val config = ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=./storage/
      akka.loglevel=OFF
      tasks.disableRemoting = true
      tasks.skipContentHashVerificationAfterCache = true
      tasks.skipContentHashCreationUponImport = true
      """
    )

    // Start the  distributed runtime
    withTaskSystem(Some(config)) { implicit tsc =>

      def parseTransactions(fileHandle: SharedFile) =
        for {
          // Returns an expression which can be combined into a series of queries and
          // eventually evaluated
          table <- ra3
            .importCsv(
              file = fileHandle,
              name = fileHandle.name,
              columns = (
                ra3.CSVColumnDefinition.StrColumn(0),
                ra3.CSVColumnDefinition.StrColumn(1),
                ra3.CSVColumnDefinition.StrColumn(2),
                ra3.CSVColumnDefinition.I32Column(3),
                ra3.CSVColumnDefinition.F64Column(4)
              ),
              maxSegmentLength = 10_000_000,
              recordSeparator = "\n",
              fieldSeparator = '\t',
              compression = Some(ra3.CompressionFormat.Gzip)
            )

          _ <- IO {
            println(table.table)
          }
          sample <- table.table.showSample(nrows = 10)
          _ <- IO {
            println(sample)
          }
        } yield table

      def avgInAndOutWithoutAbstractions(
          transactions: TableExpr[
            ReturnValueTuple[(StrVar, StrVar, StrVar, I32Var, F64Var)]
          ]
      ) = {
        // query is an expression which can be further combined or evaluated
        // .schema 'assigns variables' to the columns of a table via an appropriately typed lambda
        val query = transactions
          .schema { (customerIn, customerOut, _, _, value) => _ =>
            customerIn.groupBy
              .withPartitionBase(16)
              // partial reduction reduces a partial groups: it is not guaranteed that all elements
              // are present in the group. This is quicker to compute and for associative and
              // distributive reductions one can follow up with a total reduction
              .reducePartial(
                // :* is building a tuple of selected columns
                customerIn.first.unnamed :*
                  value.sum.unnamed :*
                  value.count.unnamed :*
                  value.min.unnamed :*
                  value.max.unnamed
              )
              // .in provides a local reference to an already evaluated table
              .in(_.tap("partial group by"))
              .schema { case (customer, sum, count, min, max) =>
                _ =>
                  customer.groupBy
                    .withPartitionBase(16)
                    .reduceTotal(
                      (customer.first `as` "customer") :*
                        ((sum.sum / count.sum) `as` "avg") :*
                        (min.min `as` "min") :*
                        (max.max `as` "max")
                    )
              }
              .schema { (customerIn, avgInValue, _, _) => _ =>
                customerOut.groupBy
                  .withPartitionBase(16)
                  .reducePartial(
                    customerOut.first.unnamed :*
                      value.sum.unnamed :*
                      value.count.unnamed :*
                      value.min.unnamed :*
                      value.max.unnamed
                  )
                  .schema { case (customer, sum, count, min, max) =>
                    _ =>
                      customer.groupBy
                        .withPartitionBase(16)
                        .reduceTotal(
                          (
                            (customer.first `as` "customer") :*
                              ((sum.sum / count.sum) `as` "avg") :*
                              (min.min `as` "min") :*
                              (max.max `as` "max")
                          )
                        )

                  }
                  .schema { (customerOut, avgOutValue, _, _) => _ =>
                    customerIn
                      .outer(customerOut)
                      .withPartitionBase(16)
                      .select(
                        ra3.select0
                          .extend(customerIn as "cIn")
                          .extend(customerOut as "cOut")
                          .extend(
                            customerIn.isMissing
                              .ifelse(
                                customerOut,
                                customerIn
                              ) as "customer"
                          )
                          .extend(avgInValue as "inAvg")
                          .extend(avgOutValue as "outAvg")
                      )
                  }

              }

          }

        // query.evaluate evaluates the query into an IO[Table] (which needs to be run as well)
        // an alternative is evaluateToStream which evaluates into an fs2.Stream of tuples
        IO { println(ra3.render(query)) }.flatMap(_ => query.evaluate)
      }

      val (transactionTabl, avgTable, avgCsv) =
        (for {
          fileHandle <- SharedFile(uri =
            tasks.util.Uri(
              s"file://${new java.io.File(path).getAbsolutePath()}"
            )
          )
          table <- parseTransactions(fileHandle)
          avgTable <- avgInAndOutWithoutAbstractions(table)
          // export.ToCsv writes a series a csv files from the final table

          exportedFiles <- avgTable.exportToCsv()
          _ <- avgTable.showSample().flatMap(sample => IO { println(sample) })
        } yield (table, avgTable, exportedFiles))
          .unsafeRunSync()

      println(transactionTabl)
      println(avgTable)
      println(avgCsv)
    }
  }

  @main
  def run(
      @arg()
      generate: Flag,
      @arg()
      size: Long = 1_000_000,
      @arg()
      path: String
  ) = {
    if (generate.value) {
      runGenerate(size, path)
    } else {
      runQuery(path)
    }
  }

  scribe
    .Logger("tasks") // Look-up or create the named logger
    .orphan() // This keeps the logger from propagating any records up the hierarchy
    .clearHandlers() // Clears any existing handlers on this logger
    .withHandler(minimumLevel =
      Some(scribe.Level.Error)
    ) // Adds a new handler to this logger
    .replace()

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Debug))
    .replace()

  def main(args: Array[String]): Unit =
    ParserForMethods(this).runOrExit(args.toIndexedSeq)

}

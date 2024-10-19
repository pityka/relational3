//> using scala 3.5.1
//> using option -experimental
//> using dep io.github.pityka::ra3-core:0.2.0+3-98f6437b+20241019-2146-SNAPSHOT
//> using dep com.typesafe.akka::akka-actor:2.6.19
//> using dep com.typesafe.akka::akka-slf4j:2.6.19
//> using dep com.typesafe.akka::akka-remote:2.6.19
import tasks.*
import cats.effect.*
import ra3.*

import com.typesafe.config.ConfigFactory

object Transactions {
  def makeQuery(
      fileHandle: SharedFile
  ) =
    for {
      transactions <- ra3
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
      avgByIn <-
        transactions
          .schema { (customerIn, customerOut, _, _, value) => _ =>
            customerIn.groupBy
              .withPartitionBase(16)
              // partial reduction reduces a partial groups: it is not guaranteed that all elements
              // are present in the group. This is quicker to compute and for associative and
              // distributive reductions one can follow up with a total reduction
              .reduceTotal(
                // :* is building a tuple of selected columns
                customerIn.first.unnamed :*
                  (value.sum / value.count).unnamed
              )
              // .in provides a local reference to an already evaluated table
              .in(_.tap("group by in looks like this "))
          }
      avgByOut <-
        transactions
          .schema { (_, customerOut, _, _, value) => _ =>
            customerOut.groupBy
              .withPartitionBase(16)
              .reduceTotal(
                customerOut.first.unnamed :*
                  (value.sum / value.count).unnamed
              )
              // .in provides a local reference to an already evaluated table
              .in(_.tap("group by out"))
          }
      joined <- avgByIn.schema { (customerIn, avgIn) => _ =>
        avgByOut.schema { (customerOut, avgOut) => _ =>
          customerIn
            .outer(customerOut)
            .withPartitionBase(16)
            .select(
              (
                customerIn.isMissing
                  .ifelse(
                    customerOut,
                    customerIn
                  ) as "customer"
              ) :* (avgIn as "inAvg") :* (avgOut as "outAvg")
            )
        }
      }

    } yield joined
  end makeQuery

}

object App extends IOApp {
  val configureScribe = IO {
    scribe.Logger.root
      .clearHandlers()
      .clearModifiers()
      .withHandler(minimumLevel = Some(scribe.Level.Info))
      .replace()
  }

  def run(args: List[String]) =
    configureScribe *> runQuery(args(0)) *> IO.pure(ExitCode.Success)

  def runQuery(path: String) = {
    println("hi")

    val config = ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=./storage/
      akka.loglevel=OFF
      tasks.disableRemoting = true
      tasks.skipContentHashVerificationAfterCache = true
      tasks.skipContentHashCreationUponImport = true
      """
    )

    // Start the  distributed runtime
    // defaultTaskSystem returns a cats effect Resource
    defaultTaskSystem(Some(config))._1.use { implicit tsc =>
      for {
        _ <- IO { println("io") }
        fileHandle <- SharedFile(uri =
          tasks.util.Uri(
            s"file://${new java.io.File(path).getAbsolutePath()}"
          )
        )
        query = Transactions.makeQuery(fileHandle)
        _ <- IO { print(query.render) }
        result <- query.evaluate
        _ <- result.showSample().flatMap(sample => IO { println(sample) })
        // export.ToCsv writes a series a csv files from the final table
        exportedFiles <- result.exportToCsv()
        _ <- IO { println(exportedFiles) }
      } yield ()

    }
  }
}

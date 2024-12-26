//> using scala 3.6.2
//> using option -experimental -language:experimental.namedTuples
//> using dep io.github.pityka::ra3-core:0.3.0+11-cd81f76d-SNAPSHOT
//> using dep com.typesafe.akka::akka-actor:2.6.19
//> using dep com.typesafe.akka::akka-slf4j:2.6.19
//> using dep com.typesafe.akka::akka-remote:2.6.19
import tasks.*
import cats.effect.*
import ra3.*
import com.typesafe.config.ConfigFactory
import scribe.modify.LogModifier

// This object contains the entrypoint of an application
// There is a second object below with the query definition
object App extends IOApp {
  val configureScribe = IO {
    scribe.Logger.root
      .clearHandlers()
      .clearModifiers()
      .withModifier(
        scribe.filter.exclude(scribe.filter.PackageNameFilter("tasks"))
      )
      .withHandler(minimumLevel = Some(scribe.Level.Info))
      .replace()

  }

  // main entry point of application
  def run(args: List[String]) =
    // first some housekeeping
    configureScribe *>
      // run the actual work
      runQuery(args(0)) *>
      // exit
      IO.pure(ExitCode.Success)

  def runQuery(path: String) = {

    val config = ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=./storage/
      akka.loglevel=OFF
      tasks.disableRemoting = true
      tasks.skipContentHashVerificationAfterCache = true
      tasks.skipContentHashCreationUponImport = true
      """
    )

    // Start the distributed runtime
    // defaultTaskSystem returns a cats effect Resource
    // Resource closes after use
    defaultTaskSystem(Some(config)).map(_._1).use { implicit tsc =>
      for {
        // lift a file path into a handle for the distributed runtime
        fileHandle <- SharedFile(uri =
          tasks.util.Uri(
            s"file://${new java.io.File(path).getAbsolutePath()}"
          )
        )
        // define query, see below
        query = Transactions.defineQuery(fileHandle)
        _ <- IO { print(query.render) }
        // evaluates into an untyped Table
        result <- query.evaluate
        // logs time measurement
        _ <- Elapsed.logResult
        _ <- IOMetricState.logResult
        // shows a sample
        _ <- result.showSample().flatMap(sample => IO { println(sample) })
        // write a series a csv files from the untype table
        exportedFiles <- result.exportToCsv()
        _ <- IO { println(exportedFiles) }
        // here it evaluates the query again (observe caching) into an fs2.Stream
        // The stream yields named tuples of the correct type
        firstItemsAsTuples <- query.evaluateToStream
          .flatMap(_.take(10).compile.toList)
        _ <- IO { println(firstItemsAsTuples) }
      } yield ()

    }
  }
}

object Transactions {

  /** Defines a query on a csv of transaction log
    * (from,to,category-as-string,category-as-int,value)
    *
    * The query returns the average inbound and outbound values It does two
    * group-by-reductions and joins them together
    *
    * This method returns a query object which can be evaluated, printed or
    * reused in building bigger queries
    */
  def defineQuery(
      fileHandle: SharedFile
  ) =
    for {
      // transactionsTable is a named tuple with selectable typed fields representing columns
      transactionsTable <- ra3
        .importCsv(
          file = fileHandle,
          name = fileHandle.name,
          // below these are compile time names carried over throughout the query definition
          columns = (
            customerIn = ra3.CSVColumnDefinition.StrColumn(0),
            customerOut = ra3.CSVColumnDefinition.StrColumn(1),
            category = ra3.CSVColumnDefinition.StrColumn(2),
            categoryId = ra3.CSVColumnDefinition.I32Column(3),
            value = ra3.CSVColumnDefinition.F64Column(4)
          ),
          maxSegmentLength = 10_000_000,
          recordSeparator = "\n",
          fieldSeparator = '\t',
          compression = Some(ra3.CompressionFormat.Gzip)
        )

      avgByOutTable <-
        transactionsTable.customerOut.groupBy
          .withPartitionBase(16)
          // reduces groups completely: yields one row per group
          .reduceTotal(
            // projection of groups
            // :* is building a tuple of selected columns
            // selected columns are always named with `as`
            transactionsTable.customerOut.first.as("customer") :*
              (transactionsTable.value.sum / transactionsTable.value.count)
                .as("avg")
          )
          // .in provides a local reference to an already evaluated table
          // tap prints a sample
          .in(_.tap("group by out"))

      avgByInTable <-
        transactionsTable.customerIn.groupBy
          .withPartitionBase(16)
          // partial reduction reduces a partial groups: it is not guaranteed that all elements
          // are present in the group. This is quicker to compute and for associative and
          // distributive reductions one can follow up with a total reduction
          .reducePartial(
            transactionsTable.customerIn.first.as("customerIn") :*
              transactionsTable.value.sum.as("sum") :*
              transactionsTable.value.count.as("count") :*
              transactionsTable.value.min.as("min") :*
              transactionsTable.value.max.as("max")
          )
          // .in provides a local reference for lexical scoping
          // the body of .in takes a reference to a table
          .in(_.tap("partial group by"))
          // .flatMap also provides a local reference for lexical scoping
          // the body of .flatMap takes a named tuple for selecting fields
          .flatMap { t =>
            t.customerIn.groupBy
              .withPartitionBase(16)
              .reduceTotal(
                (t.customerIn.first `as` "customer") :*
                  ((t.sum.sum / t.count.sum) `as` "avg") :*
                  (t.min.min `as` "min") :*
                  (t.max.max `as` "max")
              )
          }
          .in(_.tap("group by in looks like this "))

      joined <-
        avgByInTable.customer
          // joins are always equijoins
          // as such they are defined on columns
          .outer(avgByOutTable.customer)
          .withPartitionBase(16)
          .select(
            // projection of joined table
            (
              avgByInTable.customer.isMissing
                .ifelse(
                  avgByOutTable.customer,
                  avgByInTable.customer
                ) as "customer"
            ) :* (avgByInTable.avg as "inAvg") :* (avgByOutTable.avg as "outAvg")
          )

    } yield joined
  end defineQuery

}

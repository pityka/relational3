import tasks._
import tasks.jsonitersupport._
import cats.effect.unsafe.implicits.global
import org.saddle._
import cats.effect.IO
import com.typesafe.config.ConfigFactory
import scala.util.Random
object main extends App {

  scribe
    .Logger("tasks.queue") // Look-up or create the named logger
    .orphan() // This keeps the logger from propagating any records up the hierarchy
    .clearHandlers() // Clears any existing handlers on this logger
    .withHandler(minimumLevel =
      Some(scribe.Level.Error)
    ) // Adds a new handler to this logger
    .replace()

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Info))
    .replace()

  val parseA = Task[(SharedFile, Int, String), ra3.Table]("parseA", 1) {
    case (path, segmentSize, name) =>
      implicit ce =>
        path.file.use { file =>
          IO {
            val channel =
              java.nio.file.Files.newByteChannel(file.toPath)

            val table = ra3.csv
              .readHeterogeneousFromCSVChannel(
                name,
                List(
                  (0, ra3.ColumnTag.StringTag, None, None),
                  (1, ra3.ColumnTag.StringTag, None, None),
                  (2, ra3.ColumnTag.StringTag, None, None),
                  (3, ra3.ColumnTag.F64, None, None)
                ),
                channel = channel,
                header = false,
                maxSegmentLength = segmentSize,
                fieldSeparator = '\t',
                recordSeparator = "\n"
              )
              .toOption
              .get
              .mapColIndex {
                case "V0" => "rowid"
                case "V1" => "customer"
                case "V2" => "category"
                case "V3" => "value"
              }
            scribe.info(table.toString)
            table
          }
        }
  }

  if (args(0) == "generategroup") {
    val num = args(1).toInt

    def makeRow() = {
      val unique = java.util.UUID.randomUUID().toString
      val millions = Random.alphanumeric.take(4).mkString
      val hundredsOfThousands1 = Random.alphanumeric.take(3).mkString
      val float = Random.nextDouble()

      (s"$unique\t$millions\t$hundredsOfThousands1\t$float\n")
    }
    val fos1 = new java.io.FileOutputStream("generatedGroupData.txt")
    Iterator
      .continually {
        val a = makeRow()
        fos1.write(a.getBytes("US-ASCII"))
      }
      .take(num)
      .foreach(_ => ())
    fos1.close()
    System.exit(0)
  }
  if (args(0) == "generatejoin") {
    val num = args(1).toInt
    val numFiles = args(2).toInt

    0 until numFiles foreach { fileIdx =>
      def makeRow() = {
        val unique = java.util.UUID.randomUUID().toString
        val billions = Random.alphanumeric.take(6).mkString

        val float = Random.nextDouble()

        (s"$unique\t$billions\t$float\n")
      }
      val fos1 = new java.io.BufferedOutputStream(
        (
          new java.io.FileOutputStream(
            s"generatedGroupJoinData1_$num.$fileIdx.txt"
          )
        )
      )
      val fos2 = new java.io.BufferedOutputStream(
        new java.io.FileOutputStream(
          s"generatedGroupJoinData2_$num.$fileIdx.txt"
        )
      )
      Iterator
        .continually {
          val a = makeRow()
          val b = makeRow()
          fos1.write(a.getBytes("US-ASCII"))
          fos2.write(b.getBytes("US-ASCII"))
        }
        .take(num)
        .foreach(_ => ())
      fos1.close()
      fos2.close()
    }
    System.exit(0)
  }

  if (args(0) == "join") {
    val filesA = args
      .drop(2)
      .filter(_.startsWith("generatedGroupJoinData1_"))
      .map(new java.io.File(_))
      .toList
    val filesB = args
      .drop(2)
      .filter(_.startsWith("generatedGroupJoinData2_"))
      .map(new java.io.File(_))
      .toList

    val segmentSize = args(2).toInt

    val config = ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=./storage/
      akka.loglevel=OFF
      tasks.disableRemoting = true
      tasks.skipContentHashVerificationAfterCache = true
      """
    )

    withTaskSystem(Some(config)) { implicit tsc =>
      val sfA = IO
        .parSequenceN(32)(
          filesA.map(f =>
            SharedFile(uri = tasks.util.Uri(s"file://${f.getAbsolutePath()}"))
          )
        )
        .unsafeRunSync()
      val sfB = IO
        .parSequenceN(32)(
          filesB.map(f =>
            SharedFile(uri = tasks.util.Uri(s"file://${f.getAbsolutePath()}"))
          )
        )
        .unsafeRunSync()

      val tableA = IO
        .parSequenceN(32)(sfA.map { sf =>
          ra3
            .importCsv(
              sf,
              sf.name,
              List(
                ra3.CSVColumnDefinition.StrColumn(0),
                ra3.CSVColumnDefinition.StrColumn(1),
                ra3.CSVColumnDefinition.F64Column(2)
              ),
              maxSegmentLength = segmentSize,
              recordSeparator = "\n",
              fieldSeparator = '\t'
              // compression = Some(ImportCsv.Gzip)
            )
        })
        .flatMap(ra3.concatenate(_: _*))
        .unsafeRunSync()
      val tableB = IO
        .parSequenceN(32)(sfB.map { sf =>
          ra3
            .importCsv(
              sf,
              sf.name,
              List(
                ra3.CSVColumnDefinition.StrColumn(0),
                ra3.CSVColumnDefinition.StrColumn(1),
                ra3.CSVColumnDefinition.F64Column(2)
              ),
              maxSegmentLength = segmentSize,
              recordSeparator = "\n",
              fieldSeparator = '\t'
              // compression = Some(ImportCsv.Gzip)
            )
        })
        .flatMap(ra3.Table.concatenate(_: _*))
        .unsafeRunSync()

      println(tableA)
      println(tableA.showSample(nrows = 10).unsafeRunSync())
      println(tableB)
      println(tableB.showSample(nrows = 10).unsafeRunSync())

      def groupByCustomer(
          customer: ra3.ColumnVariable[ra3.DStr],
          price: ra3.ColumnVariable[ra3.DF64]
      ) =
        customer.groupBy
          .apply(ra3.select(customer.first, price.sum, price.count))
          .partial
          .in[ra3.DStr, ra3.DF64, ra3.DF64] { case (customer, sum, count) =>
            customer
              .groupBy(
                ra3.select(customer.first, (sum.sum / count.sum))
              )
              .all
          }
      case class TypedTablelet(
          rowId: ra3.ColumnVariable[ra3.DStr],
          customer: ra3.ColumnVariable[ra3.DStr],
          value: ra3.ColumnVariable[ra3.DF64]
      )

      def mylet(a: ra3.Table)(f: TypedTablelet => ra3.tablelang.TableExpr) =
        ra3.let[ra3.DStr, ra3.DStr, ra3.DF64](a) { case (c0, c1, c2) =>
          f(TypedTablelet(c0, c1, c2))
        }

      val (show, table) =
        mylet(tableA) { case tableAlet =>
          
          ra3.let[ra3.DStr, ra3.DStr, ra3.DF64](tableB) {
            case (_, customerB, priceB) =>
              groupByCustomer(tableAlet.customer, tableAlet.value)
                .in[ra3.DStr, ra3.DF64] { case (customerA, meanpriceA) =>
                  groupByCustomer(customerB, priceB).in[ra3.DStr, ra3.DF64] {
                    case (customerB, meanpriceB) =>
                      customerA.join
                        .inner(customerB)
                        .withPartitionBase(256)
                        .elementwise(
                          ra3.select(customerA, meanpriceA, meanpriceB)
                        )
                  }
                }

          }
        }.evaluate
          .flatMap(t => t.showSample(nrows = 100).map((_, t)))
          .unsafeRunSync()

      println(table)
      println(show)

      table.exportToCsv().unsafeRunSync()

    }
    System.exit(0)

  }
  if (args(0) == "group") {
    val fileA = new java.io.File(args(1))
    val segmentSize = args(2).toInt

    val config = ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=./storage/
      akka.loglevel=OFF
      tasks.disableRemoting = true
      tasks.skipContentHashVerificationAfterCache = true
      """
    )

    withTaskSystem(Some(config)) { implicit tsc =>
      val sfA = SharedFile(uri =
        tasks.util.Uri(s"file://${fileA.getAbsolutePath()}")
      ).unsafeRunSync()

      val tableA = parseA((sfA, segmentSize, "tabA"))(
        ResourceRequest(1, 1)
      ).unsafeRunSync()

      println(tableA)
      println(tableA.showSample(nrows = 1000).unsafeRunSync())

      val (show, table) = ra3
        .let[ra3.DStr, ra3.DStr, ra3.DStr, ra3.DF64](tableA) {
          case (uuid, customer, category, price) =>
            category.groupBy
              .apply(ra3.select(category.first, price.mean))
              .all
              .in[ra3.DStr, ra3.DF64] { case (category, _) =>
                ra3.query(
                  ra3.select(ra3.star).where(category.matches("aa."))
                )
              }
              .in[ra3.DStr, ra3.DF64] { case (aaCategories, _) =>
                category.join
                  .inner(aaCategories)
                  .elementwise(
                    ra3.select(uuid, customer, category, price)
                  )
              }
              .in[ra3.DStr, ra3.DStr, ra3.DStr, ra3.DF64] {
                case (
                      _,
                      filteredCustomer,
                      filteredCategory,
                      filteredPrice
                    ) =>
                  filteredCustomer.groupBy
                    .by(filteredCategory)
                    .apply(
                      ra3.select(
                        filteredCustomer.first,
                        filteredCategory.first,
                        filteredPrice.sum,
                        filteredPrice.count
                      )
                    )
                    .partial
              }
              .in[ra3.DStr, ra3.DStr, ra3.DF64, ra3.DF64] {
                case (customer, category, sum, count) =>
                  customer.groupBy
                    .by(category)
                    .apply(
                      ra3.select(
                        customer.first as "customer",
                        category.first as "category",
                        sum.sum / count.sum as "mean value"
                      )
                    )
                    .all
              }

        }
        .evaluate
        .flatMap(t => t.showSample(nrows = 10).map((_, t)))
        .unsafeRunSync()

      println(table)
      println(show)

      table.exportToCsv().unsafeRunSync()

    }
    System.exit(0)

  }

  val parseT = Task[(SharedFile, Int), ra3.Table]("parse", 1) {
    case (path, segmentSize) =>
      implicit ce =>
        path.file.use { file =>
          IO {
            val channel =
              java.nio.file.Files.newByteChannel(file.toPath)

            val table = ra3.csv
              .readHeterogeneousFromCSVChannel(
                "1brctable",
                List(
                  (0, ra3.ColumnTag.StringTag, None, None),
                  (1, ra3.ColumnTag.F64, None, None)
                  // 0 until (numCols) map (i => (i, ColumnTag.I32, None)): _*
                ),
                channel = channel,
                header = false,
                maxSegmentLength = segmentSize,
                fieldSeparator = ';',
                recordSeparator = "\n"
              )
              .toOption
              .get
              .mapColIndex {
                case "V0" => "station"
                case "V1" => "value"
              }
            scribe.info(table.toString)
            table
          }
        }
  }

  val file = new java.io.File(args.head)
  val segmentSize = args(1).toInt
  val partitionBase = args(2).toInt

  val config = ConfigFactory.parseString(
    s"""tasks.fileservice.storageURI=./storage/
      akka.loglevel=OFF
      tasks.disableRemoting = true
      """
  )

  withTaskSystem(Some(config)) { implicit tsc =>
    val sf = SharedFile(uri =
      tasks.util.Uri(s"file://${file.getAbsolutePath()}")
    ).unsafeRunSync()
    val table = parseT((sf, segmentSize))(
      ResourceRequest(1, 1)
    ).unsafeRunSync()

    val topK = table
      .topK(
        sortColumn = 1,
        ascending = false,
        k = 50,
        cdfCoverage = 0.05,
        cdfNumberOfSamplesPerSegment = 100000
      )
      .unsafeRunSync()
    println(
      topK.bufferStream.compile.toList
        .unsafeRunSync()
        .map(_.toStringFrame)
        .reduce(_ concat _)
        .withRowIndex(0)
        .colAt(0)
    )

    val result = ra3
      .let[ra3.DStr, ra3.DF64](table) { (station, value) =>
        station.groupBy
          .apply(
            ra3.select(
              station.first as "station",
              value.sum as "sum",
              value.count as "count"
            )
          )
          .partial
          .in[ra3.DStr, ra3.DF64, ra3.DF64] { (station, sum, count) =>
            station.groupBy
              .apply(
                ra3.select(
                  station.first,
                  sum.sum / count.sum
                )
              )
              .all

          }
      }
      .evaluate
      .unsafeRunSync()

    println(result)

    val resultAsFrame =
      result.bufferStream.compile.toList
        .unsafeRunSync()
        .map(_.toStringFrame)
        .reduce(_ concat _)
        .withRowIndex(0)
        .colAt(0)
        .mapValues(_.toDouble)
        .sorted

    println(resultAsFrame)

    // val expected = org.saddle.csv.CsvParser
    //   .parseFromChannel[String](
    //     channel2,
    //     fieldSeparator = ';',
    //     recordSeparator = "\n"
    //   )
    //   .toOption
    //   .get
    //   ._1
    //   .withRowIndex(0)
    //   .colAt(0)
    //   .mapValues(_.toDouble)
    //   .groupBy
    //   .combine(_.mean)
    //   .sorted
    // println(Frame(resultAsFrame,expected))
  }

}

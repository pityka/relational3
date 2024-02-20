import ra3._
import ra3.lang._
import tasks._
import tasks.jsonitersupport._
import cats.effect.unsafe.implicits.global
import org.saddle._
import cats.effect.IO
import com.typesafe.config.ConfigFactory
import scala.util.Random
import ra3.ts.ImportCsv
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
                  (0, ColumnTag.StringTag, None),
                  (1, ColumnTag.StringTag, None),
                  (2, ColumnTag.StringTag, None),
                  (3, ColumnTag.F64, None)
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
                (0, ColumnTag.StringTag, None),
                (1, ColumnTag.StringTag, None),
                (2, ColumnTag.F64, None)
              ),
              maxSegmentLength = segmentSize,
              recordSeparator = "\n",
              fieldSeparator = '\t'
              // compression = Some(ImportCsv.Gzip)
            )
        })
        .flatMap(Table.concatenate(_: _*))
        .unsafeRunSync()
      val tableB = IO
        .parSequenceN(32)(sfB.map { sf =>
          ra3
            .importCsv(
              sf,
              sf.name,
              List(
                (0, ColumnTag.StringTag, None),
                (1, ColumnTag.StringTag, None),
                (2, ColumnTag.F64, None)
              ),
              maxSegmentLength = segmentSize,
              recordSeparator = "\n",
              fieldSeparator = '\t'
              // compression = Some(ImportCsv.Gzip)
            )
        })
        .flatMap(Table.concatenate(_: _*))
        .unsafeRunSync()

      println(tableA)
      println(tableA.showSample(nrows = 10).unsafeRunSync())
      println(tableB)
      println(tableB.showSample(nrows = 10).unsafeRunSync())

      def groupByCustomer(
          customer: DelayedIdent[DStr],
          price: DelayedIdent[DF64]
      ) =
        customer.groupBy
          .apply(select(customer.first, price.sum, price.count))
          .partial
          .in[DStr, DF64, DF64] { case (_, customer, sum, count) =>
            customer
              .groupBy(
                select(customer.first, (sum.sum / count.sum))
              )
              .all
          }
      import ra3.tablelang._
      import ra3.lang._
      case class TypedTablelet(
          table: TableExpr,
          rowId: ra3.lang.DelayedIdent[DStr],
          customer: ra3.lang.DelayedIdent[DStr],
          value: ra3.lang.DelayedIdent[DF64]
      )

      def mylet(a: Table)(f: TypedTablelet => ra3.tablelang.TableExpr) =
        let[DStr, DStr, DF64](a) { case (t, c0, c1, c2) =>
          f(TypedTablelet(t, c0, c1, c2))
        }

      val (show, table) =
        mylet(tableA) { case tableAlet =>
          let[DStr, DStr, DF64](tableB) { case (_, _, customerB, priceB) =>
            groupByCustomer(tableAlet.customer, tableAlet.value)
              .in[DStr, DF64] { case (_, customerA, meanpriceA) =>
                groupByCustomer(customerB, priceB).in[DStr, DF64] {
                  case (_, customerB, meanpriceB) =>
                    customerA.join
                      .inner(customerB)
                      .withPartitionBase(256)
                      .elementwise(select(customerA, meanpriceA, meanpriceB))
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

      val (show, table) = let[DStr, DStr, DStr, DF64](tableA) {
        case (_, uuid, customer, category, price) =>
          category.groupBy
            .apply(select(category.first, price.mean))
            .all
            .in[DStr, DF64] { case (uniqueCategoriesTable, category, _) =>
              uniqueCategoriesTable.query(
                select(star).where(category.matches("aa."))
              )
            }
            .in[DStr, DF64] { case (_, aaCategories, _) =>
              category.join
                .inner(aaCategories)
                .elementwise(
                  select(uuid, customer, category, price)
                )
            }
            .in[DStr, DStr, DStr, DF64] {
              case (
                    _,
                    _,
                    filteredCustomer,
                    filteredCategory,
                    filteredPrice
                  ) =>
                filteredCustomer.groupBy
                  .by(filteredCategory)
                  .apply(
                    select(
                      filteredCustomer.first,
                      filteredCategory.first,
                      filteredPrice.sum,
                      filteredPrice.count
                    )
                  )
                  .partial
            }
            .in[DStr, DStr, DF64, DF64] {
              case (_, customer, category, sum, count) =>
                customer.groupBy
                  .by(category)
                  .apply(
                    select(
                      customer.first as "customer",
                      category.first as "category",
                      sum.sum / count.sum as "mean value"
                    )
                  )
                  .all
            }

      }.evaluate
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
                  (0, ColumnTag.StringTag, None),
                  (1, ColumnTag.F64, None)
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

    val result = let[DStr, DF64](table) { (_, station, value) =>
      station.groupBy
        .apply(
          select(
            station.first as "station",
            value.sum as "sum",
            value.count as "count"
          )
        )
        .partial
        .in[DStr, DF64, DF64] { (_, station, sum, count) =>
          station.groupBy
            .apply(
              select(
                station.first,
                sum.sum / count.sum
              )
            )
            .all

        }
    }.evaluate
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

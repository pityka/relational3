// import tasks.*
// import cats.effect.unsafe.implicits.global
// import cats.effect.IO
// import com.typesafe.config.ConfigFactory
// import scala.util.Random
// import mainargs.{main, arg, ParserForMethods, Flag}
// import ra3.{StrVar, I32Var, F64Var, select,  TableExpr,  Table}
// import ra3.lang.DelayedIdent

// object Main extends App {

//   /** Generate bogus data Each row is a transaction of some value between two
//     * customers Each row consists of:
//     *   - an id with cardinality in the millions, e.g. some customer id1
//     *   - an id with cardinality in the millions, e.g. some customer id2
//     *   - an id with cardinality in the hundred thousands e.g. some category id
//     *   - an id with cardinality in the thousands e.g. some category id
//     *   - a float value, e.g. a price
//     *
//     * The file is block gzipped
//     */
//   def runGenerate(size: Long, path: String) = {

//     def makeRow() = {
//       val millions = Random.alphanumeric.take(4).mkString
//       val millions2 = Random.alphanumeric.take(4).mkString
//       val hundredsOfThousands1 = Random.alphanumeric.take(3).mkString
//       val thousands = Random.nextInt(5000)
//       val float = Random.nextDouble()

//       (s"$millions\t$millions2\t$hundredsOfThousands1\t$thousands\t$float\n")
//     }

//     Iterator
//       .continually {
//         makeRow()

//       }
//       .grouped(100_000)
//       .take((size / 100_000).toInt)
//       .zipWithIndex
//       .foreach { case (group, idx) =>
//         val fos1 = new java.util.zip.GZIPOutputStream(
//           new java.io.FileOutputStream(path, true)
//         )
//         group.foreach { line =>
//           fos1.write(line.getBytes("US-ASCII"))
//         }
//         println((idx + 1) * 100000)
//         fos1.close
//       }

//   }

//   def runQuery(path: String) = {

//     val config = ConfigFactory.parseString(
//       s"""tasks.fileservice.storageURI=./storage/
//       akka.loglevel=OFF
//       tasks.disableRemoting = true
//       tasks.skipContentHashVerificationAfterCache = true
//       """
//     )

//     withTaskSystem(Some(config)) { implicit tsc =>
//       def parseTransactions(fileHandle: SharedFile) =
//         for {
//           table <- ra3
//             .importCsv(
//               file = fileHandle,
//               name = fileHandle.name,
//               columns = List(
//                 ra3.CSVColumnDefinition.StrColumn(0),
//                 ra3.CSVColumnDefinition.StrColumn(1),
//                 ra3.CSVColumnDefinition.StrColumn(2),
//                 ra3.CSVColumnDefinition.I32Column(3),
//                 ra3.CSVColumnDefinition.F64Column(4)
//               ),
//               maxSegmentLength = 1_000_000,
//               recordSeparator = "\n",
//               fieldSeparator = '\t',
//               compression = Some(ra3.CompressionFormat.Gzip)
//             )
//           renamed = table.mapColIndex {
//             case "V0" => "customer1"
//             case "V1" => "customer2"
//             case "V2" => "category_string"
//             case "V3" => "category_int"
//             case "V4" => "value"
//           }
//           _ <- IO {
//             println(renamed)
//           }
//           sample <- renamed.showSample(nrows = 10)
//           _ <- IO {
//             println(sample)
//           }
//         } yield renamed

//       case class TransactionsSchema(
//           customerOut: DelayedIdent[StrVar],
//           customerIn: DelayedIdent[StrVar],
//           categoryString: DelayedIdent[StrVar],
//           categoryInt: DelayedIdent[I32Var],
//           value: DelayedIdent[F64Var]
//       )
//       object TransactionsSchema {
//         def schema[R](a: ra3.Table)(
//             f: TransactionsSchema => TableExpr[R]
//         ) =
//           a.schema[StrVar, StrVar, StrVar, I32Var, F64Var].columnsTuple.all {
//             case (c0, c1, c2, c3, c4) =>
//               f(TransactionsSchema(c0, c1, c2, c3, c4))
//           }
//       }

//       case class CustomerSummary(
//           customer: DelayedIdent[StrVar],
//           avg: DelayedIdent[F64Var],
//           max: DelayedIdent[F64Var]
//       )
//       object CustomerSummary {

//         def apply(
//             customer: DelayedIdent[StrVar],
//             price: DelayedIdent[F64Var]
//         )(use: [R] => CustomerSummary => TableExpr[R]) = {
//           val summary = customer.groupBy(ra3.select0.extend(
//                 customer.first.unnamed).extend(
//                 price.sum.unnamed).extend(
//                 price.count.unnamed).extend(
//                 price.max.unnamed)
//               )
//             .partial
//             .columnsTuple.all {
//               case (customer, sum, count,  max) =>
//                 customer
//                   .groupBy(
//                     ra3.select0.extend(
//                       customer.first as "customer").extend(
//                       (sum.sum / count.sum) as "avg").extend(
//                       max.max as "max")
//                     )

//                   .all
//             }

//           summary.columnsTuple.all{ case (customer,avg,max) =>
//             use(
//               CustomerSummary(
//                 customer,
//                 avg,
//                 max
//               )
//             )
//         }

//         }

//       }

//       def avgInAndOut(transactions: Table) = {
//         val query = TransactionsSchema.schema(transactions) { transactions =>
//           TransactionsSchema(
//             transactions.categoryInt.table
//               .query((transactions.categoryInt < 3000).in(pred => where(pred)))
//           ) { transactions =>
//             CustomerSummary(
//               transactions.customerIn,
//               transactions.value
//             ) { summaryByCustomerIn =>
//               CustomerSummary(
//                 transactions.customerOut,
//                 transactions.value
//               ) { summaryByCustomerOut =>
//                 val c1 = summaryByCustomerIn.customer
//                 val c2 = summaryByCustomerOut.customer
//                 c1.join( select(
//                       c1 as "cIn",
//                       c2 as "cOut",
//                       c1.isMissing.ifelseStr(c2, c1) as "customer",
//                       summaryByCustomerIn.avg as "inAvg",
//                       summaryByCustomerOut.avg as "outAvg"
//                     ))
//                   .outer(c2)

//               }
//             }
//           }
//         }
//         IO { println(ra3.render(query)) }.flatMap(_ => query.evaluate)
//       }
//       def avgInAndOutWithoutAbstractions(transactions: Table) = {
//         val query = transactions
//           .schema[StrVar, StrVar, StrVar, StrVar, F64Var].columnsTuple.all {
//             (customerIn, customerOut, _, _, value) =>
//               customerIn.groupBy
//                 (
//                   select(
//                     customerIn.first,
//                     value.sum,
//                     value.count,
//                     value.min,
//                     value.max
//                   )
//                 )
//                 .partial
//                 .in(_.tap("partial group by"))
//                 .columns {
//                   case (customer, sum, count, min, max) =>
//                     customer
//                       .groupBy(
//                         select(
//                           customer.first as "customer",
//                           (sum.sum / count.sum) as "avg",
//                           min.min as "min",
//                           max.max as "max"
//                         )
//                       )
//                       .all
//                 }
//                 .columns {
//                   (customerIn, avgInValue, _, _) =>
//                     customerOut.groupBy
//                       (
//                         select(
//                           customerOut.first,
//                           value.sum,
//                           value.count,
//                           value.min,
//                           value.max
//                         )
//                       )
//                       .partial
//                       .columns {
//                         case (customer, sum, count, min, max) =>
//                           customer
//                             .groupBy(
//                               select(
//                                 customer.first as "customer",
//                                 (sum.sum / count.sum) as "avg",
//                                 min.min as "min",
//                                 max.max as "max"
//                               )
//                             )
//                             .all
//                       }
//                       .columns {
//                         (customerOut, avgOutValue, _, _) =>
//                           customerIn.join(select(
//                                 customerIn as "cIn",
//                                 customerOut as "cOut",
//                                 customerIn.isMissing
//                                   .ifelseStr(
//                                     customerOut,
//                                     customerIn
//                                   ) as "customer",
//                                 avgInValue as "inAvg",
//                                 avgOutValue as "outAvg"
//                               ))
//                             .outer(customerOut)
//                             .done
//                       }

//                 }

//           }

//         IO { println(ra3.render(query)) }.flatMap(_ => query.evaluate)
//       }

//       val (transactionTabl, avgTable, avgCsv) =
//         (for {
//           fileHandle <- SharedFile(uri =
//             tasks.util.Uri(
//               s"file://${new java.io.File(path).getAbsolutePath()}"
//             )
//           )
//           table <- parseTransactions(fileHandle)
//           avgTable <- avgInAndOut(table)
//           avgTable2 <- avgInAndOutWithoutAbstractions(table)
//           exportedFiles <- avgTable.exportToCsv()
//           _ <- avgTable.showSample().flatMap(sample => IO { println(sample) })
//         } yield (table, avgTable, exportedFiles))
//           .unsafeRunSync()

//       println(transactionTabl)
//       println(avgTable)
//       println(avgCsv)
//     }
//   }

//   @main
//   def run(
//       @arg()
//       generate: Flag,
//       @arg()
//       size: Long = 1_000_000,
//       @arg()
//       path: String
//   ) = {
//     if (generate.value) {
//       runGenerate(size, path)
//     } else {
//       runQuery(path)
//     }
//   }

//   scribe
//     .Logger("tasks.queue") // Look-up or create the named logger
//     .orphan() // This keeps the logger from propagating any records up the hierarchy
//     .clearHandlers() // Clears any existing handlers on this logger
//     .withHandler(minimumLevel =
//       Some(scribe.Level.Error)
//     ) // Adds a new handler to this logger
//     .replace()

//   scribe.Logger.root
//     .clearHandlers()
//     .clearModifiers()
//     .withHandler(minimumLevel = Some(scribe.Level.Info))
//     .replace()

//   ParserForMethods(this).runOrExit(args.toIndexedSeq)

// }

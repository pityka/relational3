// import cats.effect.unsafe.implicits.global
// import ra3.lang.*
// import ra3.*
// import tasks.util.TempFile
// import scala.util.Random
// import tasks.*
// import tasks.jsonitersupport.*
// import cats.effect.*

// object GroupByRandomSuite {
//   val parseA = Task[(SharedFile, Int, String), ra3.Table]("parseA", 1) {
//     case (path, segmentSize, name) =>
//       implicit ce =>
//         path.file.use { file =>
//           IO {
//             val channel =
//               java.nio.file.Files.newByteChannel(file.toPath)

//             val table = ra3.csv
//               .readHeterogeneousFromCSVChannel(
//                 name,
//                 List(
//                   (0, ColumnTag.StringTag, None),
//                   (1, ColumnTag.I64, None)
//                 ),
//                 channel = channel,
//                 header = false,
//                 maxSegmentLength = segmentSize,
//                 fieldSeparator = '\t',
//                 recordSeparator = "\n"
//               )
//               .toOption
//               .get
//               .mapColIndex {
//                 case "V0" => "rowid"
//                 case "V1" => "customer"
//               }
//             scribe.info(table.toString)
//             table
//           }
//         }
//   }
// }

// class GroupByRandomSuite extends munit.FunSuite with WithTempTaskSystem {

//   test("t") {
//     withTempTaskSystem { implicit tsc =>
//       val num = 100_000
//       def makeRow() = {
//         val unique = java.util.UUID.randomUUID().toString
//         val v = Random.nextLong(10_000)

//         (s"$unique\t$v\n")
//       }
//       val tmp = TempFile.createTempFile("txt")
//       val fos1 = new java.io.FileOutputStream(tmp)
//       Iterator
//         .continually {
//           val a = makeRow()
//           fos1.write(a.getBytes("US-ASCII"))
//         }
//         .take(num)
//         .foreach(_ => ())
//       fos1.close()

//       val sfA = SharedFile(uri =
//         tasks.util.Uri(s"file://${tmp.getAbsolutePath()}")
//       ).unsafeRunSync()

//       val tableA = GroupByRandomSuite
//         .parseA((sfA, 1000, "tabA"))(
//           ResourceRequest(1, 1)
//         )
//         .unsafeRunSync()

//       val table = schema[DStr, DI64](tableA) {
//         case (_, _, customer) =>
//           customer.groupBy.partial
//             .reduceGroupsWith(select(customer.unnamed))
//             .in[DI64] { case (_, customer) =>
//               customer.groupBy.reduceGroupsWith(
//                 select(customer.first.unnamed)
//               )
//             }

//       }.evaluate
//         .unsafeRunSync()
//         .bufferStream
//         .compile
//         .toList
//         .unsafeRunSync()
//         .map(_.toStringFrame)
//         .reduce(_ concat _)

//       assert(table.numRows == 10000)
//       assert(table.colAt(0).toVec.toSeq.sorted == (0 until 10000).toList)

//     }
//   }

// }

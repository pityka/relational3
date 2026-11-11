package ra3

import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.Duration
import tasks.ExtendedTaskSystemComponents

class PartitionByColumnSuite extends munit.FunSuite with WithTempTaskSystem {
  override val munitTimeout = Duration(180, "s")

  private def cellAt(b: Buffer, i: Int): String =
    if (b.isMissing(i)) "<NA>" else b.elementAsCharSequence(i).toString

  private def rowsOf(t: Table)(implicit
      tsc: ExtendedTaskSystemComponents
  ): List[Vector[String]] =
    t.bufferStream.compile.toList
      .unsafeRunSync()
      .flatMap { bt =>
        val n = if (bt.columns.isEmpty) 0 else bt.columns.head.length
        (0 until n).toList.map { i =>
          bt.columns.map(c => cellAt(c, i))
        }
      }

  private def sampleValueTable(
      name: String,
      segmentLength: Int
  )(implicit tsc: ExtendedTaskSystemComponents): Table = {
    val csv =
      """sample,v
        |a,1
        |b,2
        |a,3
        |c,4
        |b,5
        |a,6""".stripMargin
    csvStringToHeterogeneousTable(
      name,
      csv,
      List(
        (0, ColumnTag.StringTag, None, None),
        (1, ColumnTag.I32, None, None)
      ),
      segmentLength
    )
  }

  test("one output table per distinct key, schema preserved, sorted by key") {
    withTempTaskSystem { implicit tsc =>
      val table = sampleValueTable("p1", 2)
      val keyed = table.partitionByColumnWithKey("sample", true).unsafeRunSync()

      assertEquals(keyed.map(_._1), List("a", "b", "c"))

      keyed.foreach { case (_, part) =>
        assertEquals(part.colNames, table.colNames)
        assertEquals(part.numCols, table.numCols)
      }

      val byKey = keyed.toMap
      assertEquals(
        rowsOf(byKey("a")).map(_(1)).sorted,
        List("1", "3", "6")
      )
      assertEquals(rowsOf(byKey("b")).map(_(1)).sorted, List("2", "5"))
      assertEquals(rowsOf(byKey("c")).map(_(1)).sorted, List("4"))
    }
  }

  test("round-trips: partition then concatenate yields the same row set") {
    withTempTaskSystem { implicit tsc =>
      val table = sampleValueTable("p2", 2)
      val parts = table.partitionByColumn("sample", true).unsafeRunSync()
      val recombined = ra3.concatenate(parts*).unsafeRunSync()

      assertEquals(
        rowsOf(recombined).toSet,
        rowsOf(table).toSet
      )
      assertEquals(recombined.numRows, table.numRows)
    }
  }

  test("by index agrees with by name") {
    withTempTaskSystem { implicit tsc =>
      val table = sampleValueTable("p3", 3)
      val byName = table.partitionByColumnWithKey("sample", true).unsafeRunSync()
      val byIndex = table.partitionByColumnWithKey(0, true).unsafeRunSync()
      assertEquals(byName.map(_._1), byIndex.map(_._1))
    }
  }

  test("missing key values form their own group") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """sample,v
          |a,1
          |NA,7
          |b,2
          |NA,8""".stripMargin
      val table = csvStringToHeterogeneousTable(
        "p4",
        csv,
        List(
          (0, ColumnTag.StringTag, None, Some("NA")),
          (1, ColumnTag.I32, None, None)
        ),
        2
      )
      val keyed = table.partitionByColumnWithKey("sample", true).unsafeRunSync()

      assertEquals(keyed.size, 3)
      assertEquals(keyed.head._1, MissingString)
      assertEquals(keyed.map(_._1).tail, List("a", "b"))

      val missingPart = keyed.head._2
      assertEquals(rowsOf(missingPart).map(_(1)).sorted, List("7", "8"))

      val recombined =
        ra3.concatenate(keyed.map(_._2)*).unsafeRunSync()
      assertEquals(rowsOf(recombined).toSet, rowsOf(table).toSet)
    }
  }

  test("single distinct key yields one table") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """sample,v
          |z,1
          |z,2
          |z,3""".stripMargin
      val table = csvStringToHeterogeneousTable(
        "p5",
        csv,
        List(
          (0, ColumnTag.StringTag, None, None),
          (1, ColumnTag.I32, None, None)
        ),
        2
      )
      val parts = table.partitionByColumn("sample", true).unsafeRunSync()
      assertEquals(parts.size, 1)
      assertEquals(parts.head.numRows, 3L)
    }
  }

  test("empty table yields empty list") {
    withTempTaskSystem { implicit tsc =>
      val csv = "sample,v"
      val table = csvStringToHeterogeneousTable(
        "p6",
        csv,
        List(
          (0, ColumnTag.StringTag, None, None),
          (1, ColumnTag.I32, None, None)
        ),
        2
      )
      assertEquals(table.partitionByColumn("sample", true).unsafeRunSync(), Nil)
    }
  }

  test("integer key is sorted numerically through the shuffle path") {
    withTempTaskSystem { implicit tsc =>
      val csv =
        """k,v
          |10,a
          |2,b
          |10,c
          |2,d
          |100,e""".stripMargin
      val table = csvStringToHeterogeneousTable(
        "p7",
        csv,
        List(
          (0, ColumnTag.I32, None, None),
          (1, ColumnTag.StringTag, None, None)
        ),
        2
      )
      val keyed = table
        .partitionByColumnWithKey(0, true, 4, 1, 1000)
        .unsafeRunSync()
      assertEquals(keyed.map(_._1), List("2", "10", "100"))

      val recombined = ra3.concatenate(keyed.map(_._2)*).unsafeRunSync()
      assertEquals(rowsOf(recombined).toSet, rowsOf(table).toSet)
    }
  }

  test("package level partitionByColumn delegates to the table method") {
    withTempTaskSystem { implicit tsc =>
      val table = sampleValueTable("p8", 2)
      val viaPackage =
        ra3.partitionByColumn(table, "sample", true).unsafeRunSync()
      assertEquals(viaPackage.size, 3)
    }
  }

  test("sort = false returns the same row set and one table per key") {
    withTempTaskSystem { implicit tsc =>
      val table = sampleValueTable("p9", 2)
      val keyed = table.partitionByColumnWithKey("sample", false).unsafeRunSync()
      assertEquals(keyed.map(_._1).toSet, Set("a", "b", "c"))

      val recombined = ra3.concatenate(keyed.map(_._2)*).unsafeRunSync()
      assertEquals(rowsOf(recombined).toSet, rowsOf(table).toSet)
    }
  }
}

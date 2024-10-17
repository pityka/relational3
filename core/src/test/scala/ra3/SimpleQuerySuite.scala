package ra3
import org.saddle.*

import cats.effect.unsafe.implicits.global
import ColumnTag.I32
import ra3.lang.*
import ra3.lang.util.*
import ra3.lang.Expr.DelayedIdent
import ra3.MissingString
import ra3.DInst
class SimpleQuerySuite extends munit.FunSuite with WithTempTaskSystem {
  test("test predicates of Double") {
    withTempTaskSystem { implicit ts =>
      val (tableFrame, tableCsv) = {
        val frame = Frame(
          Vec(0d, 1d, 2d, 3d, Double.NaN, 5d, 6d, 700d, 8d, -9d),
          Vec(10d, 1d, Double.NaN, 13d, 14d, 15d, 16d, 17d, 18d, -19d)
        )
        val csv = new String(
          org.saddle.csv.CsvWriter.writeFrameToArray(frame, withRowIx = false)
        )
        (frame, csv)
      }
      val ra3Table = csvStringToDoubleTable("table", tableCsv, 2, 3)

      def predicateTest(tag: String)(
          p: (DelayedIdent[DF64], DelayedIdent[DF64]) => I32ColumnExpr
      )(p2: (Vec[Double], Vec[Double]) => Array[Int]) = {
        val less = ra3Table
          .as[(F64Var, F64Var)]
          .schema { (col0, col1) => schema =>
            query(
              schema.all
                .where(p(col0, col1))
            )

          }
          .evaluate
          .unsafeRunSync()
        val takenF = toFrame2(less, ColumnTag.F64)

        val expect =
          tableFrame
            .rowAt(p2(tableFrame.colAt(0).toVec, tableFrame.colAt(1).toVec))
            .resetRowIndex
            .setColIndex(Index("V0", "V1"))

        assertEquals(takenF, expect, tag)
      }

      predicateTest("contains")((col0, _) => col0.containedIn(Set(0d, 7d)))(
        (col0, _) => col0.find(i => Set(0d, 7d).contains(i)).toArray
      )
      predicateTest("!==")((col0, _) => col0.!==(0))((col0, _) =>
        col0.find(i => i != 0).toArray
      )

      predicateTest(">=")((col0, _) => col0.abs >= 9)((col0, _) =>
        col0.find(i => math.abs(i) >= 9).toArray
      )
      predicateTest("is missing")((col0, _) => col0.isMissing)((col0, _) =>
        col0.toArray.zipWithIndex.filter(_._1.isNaN()).map(_._2)
      )

      predicateTest("!== not")((col0, _) => col0.!==(0).not)((col0, _) =>
        col0.find(i => i == 0).toArray
      )
      predicateTest("===")((col0, _) => col0.===(0))((col0, _) =>
        col0.find(i => i == 0).toArray
      )
      predicateTest(">=")((col0, _) => col0 >= 5)((col0, _) =>
        col0.find(i => i >= 5).toArray
      )
      predicateTest("<= const")((col0, _) => col0 <= 5)((col0, _) =>
        col0.find(i => i <= 5).toArray
      )
      predicateTest("<")((col0, _) => col0 < 5)((col0, _) =>
        col0.find(i => i < 5).toArray
      )
      predicateTest(">")((col0, _) => col0 > 5)((col0, _) =>
        col0.find(i => i > 5).toArray
      )

      predicateTest("||")((col0, _) => col0 > 5 || col0 < 2)((col0, _) =>
        col0.find(i => i > 5 || i < 2).toArray
      )
      predicateTest("&&")((col0, col1) => col0 > 5 && col1 >= 18)(
        (col0, col1) =>
          (col0.find(i => i > 5).toSeq.toSet & col1
            .find(_ >= 18)
            .toSeq
            .toSet).toArray
      )
      predicateTest(">=")((col0, col1) => col0 >= col1)((col0, col1) =>
        col0.zipMap(col1)(_ >= _).find(identity).toArray
      )
      predicateTest(">")((col0, col1) => col0 > col1)((col0, col1) =>
        col0.zipMap(col1)(_ > _).find(identity).toArray
      )
      predicateTest("<=")((col0, col1) => col0 <= col1)((col0, col1) =>
        col0.zipMap(col1)(_ <= _).find(identity).toArray
      )
      predicateTest("<")((col0, col1) => col0 < col1)((col0, col1) =>
        col0.zipMap(col1)(_ < _).find(identity).toArray
      )
      predicateTest("===")((col0, col1) => col0 === col1)((col0, col1) =>
        col0.zipMap(col1)(_ == _).find(identity).toArray
      )
      predicateTest("!==")((col0, col1) => col0 !== col1)((col0, col1) =>
        col0.zipMap(col1)(_ != _).find(identity).toArray
      )
      predicateTest("===")((col0, _) => col0.roundToInt === 0)((col0, _) =>
        col0.find(i => i.toDouble == 0).toArray
      )

    }

  }

  test("test predicates of String") {
    withTempTaskSystem { implicit ts =>
      val (tableFrame, tableCsv) = {
        val frame = Frame(
          Vec(0, 1, 2, 3, Int.MinValue, 5, 6, 700, 8, -9),
          Vec(10, 1, Int.MinValue, 13, 14, 15, 16, 17, 18, -19)
        )
        val csv = new String(
          org.saddle.csv.CsvWriter.writeFrameToArray(frame, withRowIx = false)
        )
        (frame.mapValues(_.toString), csv)
      }
      val ra3Table = csvStringToStringTable("table", tableCsv, 2, 3)
        .as[(StrVar, StrVar)]
        .schema((col0, col1) =>
          _ =>
            query(
              ra3.select0
                .extend(col0.matchAndReplace("NA", MissingString) as "V0")
                .extend(
                  col1.matchAndReplace("NA", MissingString) as "V1"
                )
            )
        )
        .evaluate
        .unsafeRunSync()
      def predicateTest(tag: String)(
          p: (DelayedIdent[DStr], DelayedIdent[DStr]) => I32ColumnExpr
      )(p2: (Vec[String], Vec[String]) => Array[Int]) = {
        val less = ra3Table
          .as[(StrVar, StrVar)]
          .schema((col0, col1) =>
            schema => query(schema.all.where(p(col0, col1)))
          )
          .evaluate
          .unsafeRunSync()
        val takenF = toFrame2(less, ColumnTag.StringTag)
          .mapValues(_.toString)
          .mapValues(x => if (x == MissingString) null else x)
          .mapRowIndex(_ => "_")

        val expect =
          tableFrame
            .rowAt(p2(tableFrame.colAt(0).toVec, tableFrame.colAt(1).toVec))
            .resetRowIndex
            .setColIndex(Index("V0", "V1"))
            .mapRowIndex(_ => "_")

        assertEquals(takenF, expect, tag)
      }

      predicateTest("contains")((col0, _) => col0.containedIn(Set("0", "7")))(
        (col0, _) => col0.find(i => Set("0", "7").contains(i)).toArray
      )
      predicateTest("!== 0")((col0, _) => col0.!==("0"))((col0, _) =>
        col0.find(i => i != "0").toArray
      )

      predicateTest("is missing")((col0, _) => col0.isMissing)((col0, _) =>
        col0.toArray.zipWithIndex.filter(_._1 == null).map(_._2)
      )

      predicateTest("!== not")((col0, _) => col0.!==("0").not)((col0, _) =>
        col0.find(i => i == "0").toArray
      )
      predicateTest("===")((col0, _) => col0.===("0"))((col0, _) =>
        col0.find(i => i == "0").toArray
      )
      predicateTest(">=")((col0, _) => col0 >= "5")((col0, _) =>
        col0.find(i => i >= "5").toArray
      )
      predicateTest("<=")((col0, _) => col0 <= "5")((col0, _) =>
        col0.find(i => i <= "5").toArray
      )
      predicateTest("<")((col0, _) => col0 < "5")((col0, _) =>
        col0.find(i => i < "5").toArray
      )
      predicateTest(">")((col0, _) => col0 > "5")((col0, _) =>
        col0.find(i => i > "5").toArray
      )

      predicateTest("||")((col0, _) => col0 > "5" || col0 < "2")((col0, _) =>
        col0.find(i => i > "5" || i < "2").toArray
      )
      predicateTest("&&")((col0, col1) => col0 > "5" && col1 >= "18")(
        (col0, col1) =>
          (col0.find(i => i > "5").toSeq.toSet & col1
            .find(_ >= "18")
            .toSeq
            .toSet).toArray
      )
      predicateTest(">=")((col0, col1) => col0 >= col1)((col0, col1) =>
        col0.zipMap(col1)(_ >= _).find(identity).toArray
      )
      predicateTest(">")((col0, col1) => col0 > col1)((col0, col1) =>
        col0.zipMap(col1)(_ > _).find(identity).toArray
      )
      predicateTest("<=")((col0, col1) => col0 <= col1)((col0, col1) =>
        col0.zipMap(col1)(_ <= _).find(identity).toArray
      )
      predicateTest("<")((col0, col1) => col0 < col1)((col0, col1) =>
        col0.zipMap(col1)(_ < _).find(identity).toArray
      )
      predicateTest("===")((col0, col1) => col0 === col1)((col0, col1) =>
        col0.zipMap(col1)(_ == _).find(identity).toArray
      )
      predicateTest("!== col")((col0, col1) => col0 !== col1)((col0, col1) =>
        col0.zipMap(col1)(_ != _).find(identity).toArray
      )
      predicateTest("===")((col0, _) => col0.toDouble === 0d)((col0, _) =>
        col0.find(i => i.toDouble == 0d).toArray
      )

    }

  }
  test("test predicates of Long") {
    withTempTaskSystem { implicit ts =>
      val (tableFrame, tableCsv) = {
        val frame = Frame(
          Vec(0L, 1L, 2L, 3L, Long.MinValue, 5L, 6L, 700L, 8L, -9L),
          Vec(10L, 1L, Long.MinValue, 13L, 14L, 15L, 16L, 17L, 18L, -19L)
        )
        val csv = new String(
          org.saddle.csv.CsvWriter.writeFrameToArray(frame, withRowIx = false)
        )
        (frame, csv)
      }
      val ra3Table = csvStringToLongTable("table", tableCsv, 2, 3)

      def predicateTest(tag: String)(
          p: (DelayedIdent[DI64], DelayedIdent[DI64]) => I32ColumnExpr
      )(p2: (Vec[Long], Vec[Long]) => Array[Int]) = {
        val less = ra3Table
          .as[(I64Var, I64Var)]
          .schema((col0, col1) =>
            schema =>
              query(
                schema.all
                  .where(p(col0, col1))
              )
          )
          .evaluate
          .unsafeRunSync()
        val takenF = toFrame2(less, ColumnTag.I64)

        val expect =
          tableFrame
            .rowAt(p2(tableFrame.colAt(0).toVec, tableFrame.colAt(1).toVec))
            .resetRowIndex
            .setColIndex(Index("V0", "V1"))

        assertEquals(takenF, expect, tag)
      }

      // predicateTest("contains")((col0, _) => col0.containedIn(Set(0, 7)))(
      //   (col0, _) => col0.find(i => Set(0, 7).contains(i)).toArray
      // )
      // predicateTest("!==")((col0, _) => col0.!==(0))((col0, _) =>
      //   col0.find(i => i != 0).toArray
      // )

      // predicateTest(">=")((col0, _) => col0.abs >= 9)((col0, _) =>
      //   col0.find(i => math.abs(i) >= 9).toArray
      // )
      // predicateTest("is missing")((col0, _) => col0.isMissing)((col0, _) =>
      //   col0.toArray.zipWithIndex.filter(_._1 == Int.MinValue).map(_._2)
      // )

      // predicateTest("!== not")((col0, _) => col0.!==(0).not)((col0, _) =>
      //   col0.find(i => i == 0).toArray
      // )
      predicateTest("===")((col0, _) => col0.===(0))((col0, _) =>
        col0.find(i => i == 0).toArray
      )
      // predicateTest(">=")((col0, _) => col0 >= 5)((col0, _) =>
      //   col0.find(i => i >= 5).toArray
      // )
      // predicateTest("<=")((col0, _) => col0 <= 5)((col0, _) =>
      //   col0.find(i => i <= 5).toArray
      // )
      // predicateTest("<")((col0, _) => col0 < 5)((col0, _) =>
      //   col0.find(i => i < 5).toArray
      // )
      // predicateTest(">")((col0, _) => col0 > 5)((col0, _) =>
      //   col0.find(i => i > 5).toArray
      // )

      // predicateTest("||")((col0, _) => col0 > 5 || col0 < 2)((col0, _) =>
      //   col0.find(i => i > 5 || i < 2).toArray
      // )
      // predicateTest("&&")((col0, col1) => col0 > 5 && col1 >= 18)(
      //   (col0, col1) =>
      //     (col0.find(i => i > 5).toSeq.toSet & col1
      //       .find(_ >= 18)
      //       .toSeq
      //       .toSet).toArray
      // )
      // predicateTest(">=")((col0, col1) => col0 >= col1)((col0, col1) =>
      //   col0.zipMap(col1)(_ >= _).find(identity).toArray
      // )
      // predicateTest(">")((col0, col1) => col0 > col1)((col0, col1) =>
      //   col0.zipMap(col1)(_ > _).find(identity).toArray
      // )
      // predicateTest("<=")((col0, col1) => col0 <= col1)((col0, col1) =>
      //   col0.zipMap(col1)(_ <= _).find(identity).toArray
      // )
      // predicateTest("<")((col0, col1) => col0 < col1)((col0, col1) =>
      //   col0.zipMap(col1)(_ < _).find(identity).toArray
      // )
      predicateTest("===")((col0, col1) => col0 === col1)((col0, col1) =>
        col0.zipMap(col1)(_ == _).find(identity).toArray
      )
      // predicateTest("!==")((col0, col1) => col0 !== col1)((col0, col1) =>
      //   col0.zipMap(col1)(_ != _).find(identity).toArray
      // )
      // predicateTest("===")((col0, _) => col0.toDouble === 0d)((col0, _) =>
      //   col0.find(i => i.toDouble == 0d).toArray
      // )

    }

  }
  test("test predicates of Instant") {
    withTempTaskSystem { implicit ts =>
      val (tableFrame, tableCsv) = {
        val frame = Frame(
          Vec(0L, 1L, 2L, 3L, Long.MinValue, 5L, 6L, 700L, 8L, -9L),
          Vec(10L, 1L, Long.MinValue, 13L, 14L, 15L, 16L, 17L, 18L, -19L)
        )
        val csv = new String(
          org.saddle.csv.CsvWriter.writeFrameToArray(frame, withRowIx = false)
        )
        (frame, csv)
      }
      val ra3Table = csvStringToLongTable("table", tableCsv, 2, 3)

      def predicateTest(tag: String)(
          p: (Expr[DInst], Expr[DInst]) => I32ColumnExpr
      )(p2: (Vec[Long], Vec[Long]) => Array[Int]) = {
        val less = ra3Table
          .as[(I64Var, I64Var)]
          .schema((col0, col1) =>
            schema =>
              query(
                schema.none
                  .extend(col0.toInstantEpochMilli)
                  .extend(col1.toInstantEpochMilli)
                  .where(p(col0.toInstantEpochMilli, col1.toInstantEpochMilli))
              )
          )
          .evaluate
          .unsafeRunSync()
        val takenF = toFrame2(less, ColumnTag.I64)

        val expect =
          tableFrame
            .rowAt(p2(tableFrame.colAt(0).toVec, tableFrame.colAt(1).toVec))
            .resetRowIndex
            .setColIndex(Index("V0", "V1"))

        assertEquals(takenF, expect, tag)
      }

      // predicateTest("contains")((col0, _) => col0.containedIn(Set(0, 7)))(
      //   (col0, _) => col0.find(i => Set(0, 7).contains(i)).toArray
      // )
      predicateTest("!==")((col0, _) => col0.!==(0))((col0, _) =>
        col0.find(i => i != 0).toArray
      )

      predicateTest(">=")((col0, _) => col0 >= 9)((col0, _) =>
        col0.find(i => i >= 9).toArray
      )
      predicateTest("is missing")((col0, _) => col0.isMissing)((col0, _) =>
        col0.toArray.zipWithIndex.filter(_._1 == Long.MinValue).map(_._2)
      )

      predicateTest("!== not")((col0, _) => col0.!==(0).not)((col0, _) =>
        col0.find(i => i == 0).toArray
      )
      predicateTest("===")((col0, _) => col0.===(0L))((col0, _) =>
        col0.find(i => i == 0).toArray
      )
      predicateTest(">=")((col0, _) => col0 >= 5)((col0, _) =>
        col0.find(i => i >= 5).toArray
      )
      predicateTest("<=")((col0, _) => col0 <= 5)((col0, _) =>
        col0.find(i => i <= 5).toArray
      )
      predicateTest("<")((col0, _) => col0 < 5)((col0, _) =>
        col0.find(i => i < 5).toArray
      )
      predicateTest(">")((col0, _) => col0 > 5)((col0, _) =>
        col0.find(i => i > 5).toArray
      )

      predicateTest(">=")((col0, col1) => col0 >= col1)((col0, col1) =>
        col0.zipMap(col1)(_ >= _).find(identity).toArray
      )
      predicateTest(">")((col0, col1) => col0 > col1)((col0, col1) =>
        col0.zipMap(col1)(_ > _).find(identity).toArray
      )
      predicateTest("<=")((col0, col1) => col0 <= col1)((col0, col1) =>
        col0.zipMap(col1)(_ <= _).find(identity).toArray
      )
      predicateTest("<")((col0, col1) => col0 < col1)((col0, col1) =>
        col0.zipMap(col1)(_ < _).find(identity).toArray
      )
      predicateTest("===")((col0, col1) => col0 === col1)((col0, col1) =>
        col0.zipMap(col1)(_ == _).find(identity).toArray
      )
      predicateTest("!==")((col0, col1) => col0 !== col1)((col0, col1) =>
        col0.zipMap(col1)(_ != _).find(identity).toArray
      )
      // predicateTest("===")((col0, _) => col0.toDouble === 0d)((col0, _) =>
      //   col0.find(i => i.toDouble == 0d).toArray
      // )

    }

  }
  test("test predicates of Int") {
    withTempTaskSystem { implicit ts =>
      val (tableFrame, tableCsv) = {
        val frame = Frame(
          Vec(0, 1, 2, 3, Int.MinValue, 5, 6, 700, 8, -9),
          Vec(10, 1, Int.MinValue, 13, 14, 15, 16, 17, 18, -19)
        )
        val csv = new String(
          org.saddle.csv.CsvWriter.writeFrameToArray(frame, withRowIx = false)
        )
        (frame, csv)
      }
      val ra3Table = csvStringToTable("table", tableCsv, 2, 3)

      def predicateTest(tag: String)(
          p: (DelayedIdent[DI32], DelayedIdent[DI32]) => I32ColumnExpr
      )(p2: (Vec[Int], Vec[Int]) => Array[Int]) = {
        val less = ra3Table
          .as[(I32Var, I32Var)]
          .schema((col0, col1) =>
            schema =>
              query(
                schema.all
                  .where(p(col0, col1))
              )
          )
          .evaluate
          .unsafeRunSync()
        val takenF = toFrame2(less, I32)

        val expect =
          tableFrame
            .rowAt(p2(tableFrame.colAt(0).toVec, tableFrame.colAt(1).toVec))
            .resetRowIndex
            .setColIndex(Index("V0", "V1"))

        assertEquals(takenF, expect, tag)
      }

      predicateTest("contains")((col0, _) => col0.containedIn(Set(0, 7)))(
        (col0, _) => col0.find(i => Set(0, 7).contains(i)).toArray
      )
      predicateTest("!==")((col0, _) => col0.!==(0))((col0, _) =>
        col0.find(i => i != 0).toArray
      )

      predicateTest(">=")((col0, _) => col0.abs >= 9)((col0, _) =>
        col0.find(i => math.abs(i) >= 9).toArray
      )
      predicateTest("is missing")((col0, _) => col0.isMissing)((col0, _) =>
        col0.toArray.zipWithIndex.filter(_._1 == Int.MinValue).map(_._2)
      )

      predicateTest("!== not")((col0, _) => col0.!==(0).not)((col0, _) =>
        col0.find(i => i == 0).toArray
      )
      predicateTest("===")((col0, _) => col0.===(0))((col0, _) =>
        col0.find(i => i == 0).toArray
      )
      predicateTest(">=")((col0, _) => col0 >= 5)((col0, _) =>
        col0.find(i => i >= 5).toArray
      )
      predicateTest("<=")((col0, _) => col0 <= 5)((col0, _) =>
        col0.find(i => i <= 5).toArray
      )
      predicateTest("<")((col0, _) => col0 < 5)((col0, _) =>
        col0.find(i => i < 5).toArray
      )
      predicateTest(">")((col0, _) => col0 > 5)((col0, _) =>
        col0.find(i => i > 5).toArray
      )

      predicateTest("||")((col0, _) => col0 > 5 || col0 < 2)((col0, _) =>
        col0.find(i => i > 5 || i < 2).toArray
      )
      predicateTest("&&")((col0, col1) => col0 > 5 && col1 >= 18)(
        (col0, col1) =>
          (col0.find(i => i > 5).toSeq.toSet & col1
            .find(_ >= 18)
            .toSeq
            .toSet).toArray
      )
      predicateTest(">=")((col0, col1) => col0 >= col1)((col0, col1) =>
        col0.zipMap(col1)(_ >= _).find(identity).toArray
      )
      predicateTest(">")((col0, col1) => col0 > col1)((col0, col1) =>
        col0.zipMap(col1)(_ > _).find(identity).toArray
      )
      predicateTest("<=")((col0, col1) => col0 <= col1)((col0, col1) =>
        col0.zipMap(col1)(_ <= _).find(identity).toArray
      )
      predicateTest("<")((col0, col1) => col0 < col1)((col0, col1) =>
        col0.zipMap(col1)(_ < _).find(identity).toArray
      )
      predicateTest("===")((col0, col1) => col0 === col1)((col0, col1) =>
        col0.zipMap(col1)(_ == _).find(identity).toArray
      )
      predicateTest("!==")((col0, col1) => col0 !== col1)((col0, col1) =>
        col0.zipMap(col1)(_ != _).find(identity).toArray
      )
      predicateTest("===")((col0, _) => col0.toDouble === 0d)((col0, _) =>
        col0.find(i => i.toDouble == 0d).toArray
      )

    }

  }
  test("simple query op containedIn Set[String]") {
    withTempTaskSystem { implicit ts =>
      val (tableFrame, tableCsv) = {
        val frame = Frame(
          Vec(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
          Vec(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
        )
        val csv = new String(
          org.saddle.csv.CsvWriter.writeFrameToArray(frame, withRowIx = false)
        )
        (frame, csv)
      }
      val ra3Table = csvStringToStringTable("table", tableCsv, 2, 3)
      val less =
        ra3Table
          .as[(StrVar, StrVar)]
          .schema((col0, col1) =>
            schema =>
              query(
                schema.all
                  .where(col0.tap("col0").containedIn(Set("1", "2")))
              )
          )
          .evaluate
          .unsafeRunSync()
      println(less)
      val takenF = (0 until 4)
        .map(i => less.bufferSegment(i).unsafeRunSync().toStringFrame)
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)

      val expect =
        tableFrame
          .rowAt(
            tableFrame.colAt(0).toVec.find(i => Set(1, 2).contains(i)).toArray
          )
          .resetRowIndex
          .setColIndex(Index("V0", "V1"))
          .mapValues(_.toString)

      assertEquals(takenF, expect)
    }

  }
  test("ifelse int") {
    withTempTaskSystem { implicit ts =>
      val (_, tableCsv) = {
        val frame = Frame(
          Vec(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
          Vec(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
        )
        val csv = new String(
          org.saddle.csv.CsvWriter.writeFrameToArray(frame, withRowIx = false)
        )
        (frame, csv)
      }
      val ra3Table = csvStringToTable("table", tableCsv, 2, 3)
      val less =
        ra3Table
          .as[(I32Var, I32Var)]
          .schema((col0, col1) =>
            schema =>
              query(
                schema.none.extend(col0.ifelse(col0, col1))
              )
          )
          .evaluate
          .unsafeRunSync()
      val takenF = (0 until 4)
        .map(i => less.bufferSegment(i).unsafeRunSync().toStringFrame)
        .reduce(_ concat _)
        .resetRowIndex
        .filterIx(_.nonEmpty)

      val expect =
        Frame("V0" -> Vec(10, 1, 2, 3, 4, 5, 6, 7, 8, 9).map(_.toString))

      assertEquals(takenF, expect)
    }

  }
}

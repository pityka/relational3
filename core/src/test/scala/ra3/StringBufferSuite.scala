package ra3

import cats.effect.unsafe.implicits.global
import java.nio.CharBuffer

class StringBufferSuite extends munit.FunSuite with WithTempTaskSystem {
      test("makeStatistic") {
      val s = Seq("0", "1", "2", "3", BufferString.MissingValue, "4")
      val st = BufferString(s.toArray).makeStatistic()
      assertEquals(st.hasMissing, true)
      assertEquals(st.nonMissingMinMax, Some("0" -> "4"))
      assertEquals(st.lowCardinalityNonMissingSet, Some(Set[CharSequence]("0","1","2","3","4")))

  }
  test("makeStatistic long") {
      val s = Seq(0 until 256:_*).map(_.toString)
      val st = BufferString(s: _*).makeStatistic()
      assertEquals(st.hasMissing, false)
      assertEquals(st.nonMissingMinMax, Some("0" -> "99"))
      assertEquals(st.lowCardinalityNonMissingSet, None)
  }
  test("toSegment") {
    withTempTaskSystem { implicit tsc =>
      val s: Seq[CharSequence] =
        Seq("0", "1", "2", "3", s"${Char.MinValue}", ".", "")
      val segment = BufferString(s.toArray)
        .toSegment(LogicalPath("toSegmentTest", None, 0, 0))
        .unsafeRunSync()
      assertEquals(segment.buffer.unsafeRunSync().toSeq.map(_.toString), s)
      assertEquals(segment.numElems, 7)
      assertEquals(segment.nonMissingMinMax.get, ("", "3"))
      assertEquals(segment.statistic.hasMissing, true)
    }
  }
  test("findInequalityVsHead") {
    val b1 =
      BufferString(Array[CharSequence]("0", "1", "2", "3", "4", "4", "5", "6"))
    val b2 = BufferString(Array(BufferString.missing))
    val b3 = BufferString(Array[CharSequence]("0"))
    assertEquals(b1.findInequalityVsHead(b2, true).toSeq, Nil)
    assertEquals(b1.findInequalityVsHead(b3, true).toSeq, List(0))
    assertEquals(
      b1.findInequalityVsHead(b3, false).toSeq,
      List(0, 1, 2, 3, 4, 5, 6, 7)
    )
  }
  test("cdf") {
    val b1 =
      BufferString(Array[CharSequence]("0", "1", "2", "3", "4", "4", "5", "6"))
    assertEquals(b1.cdf(2)._1.values.toSeq, List("0", "6"))
    assertEquals(b1.cdf(2)._2.values.toSeq, List(0d, 1d))
    assertEquals(b1.cdf(4)._1.values.toSeq, List("0", "2", "4", "6"))
    assertEquals(b1.cdf(4)._2.values.toSeq, List(0d, 1d / 3d, 2d / 3d, 1d))
  }
  test("length") {
    val b1 =
      BufferString(Array[CharSequence]("0", "1", "2", "3", "4", "4", "5", "6"))
    assertEquals(b1.length, 8)
  }
  test("groups") {
    val b1 = BufferString(
      Array[CharSequence]("0", "1", "2", "3", "4", "4", "5", "6").reverse
    )
    assertEquals(b1.groups.map.toSeq, List(0, 1, 2, 2, 3, 4, 5, 6))
    assertEquals(b1.groups.groupSizes.toSeq, List(1, 1, 2, 1, 1, 1, 1))
    assertEquals(b1.groups.numGroups, 7)
  }
  test("take") {
    val b1 = BufferString(
      Array("99", "1", "2", BufferString.missing, "4", "4", "5", "6")
    )
    assertEquals(
      b1.take(BufferInt(-1, 0, 3, 0, -1)).toSeq,
      Seq(
        BufferString.missing,
        "99",
        BufferString.missing,
        "99",
        BufferString.missing
      )
    )
  }
  test("positiveLocations") {
    val b1 = BufferString(
      Array(BufferString.missing, "1", "2", "", "4", "4", "5", "6")
    )
    assertEquals(b1.positiveLocations.toSeq, Seq(1, 2, 4, 5, 6, 7))
  }
  test("outer join") {
    val (a, b) = BufferString("0", BufferString.missing.toString, "1", "-99")
      .computeJoinIndexes(
        BufferString(
          "0",
          "0",
          BufferString.missing.toString,
          BufferString.missing.toString,
          "1",
          "1",
          "99"
        ),
        "outer"
      )
    assertEquals(a.get.toSeq, Seq(0, 0, 1, 2, 2, 3, -1, -1 ,-1))
    assertEquals(b.get.toSeq, Seq(0, 1, -1, 4, 5, -1, 2,3,6))
  }

  test("mergeNonMissing") {
    assertEquals(
      BufferString(
        "0",
        BufferString.missing.toString,
        "0",
        "1",
        BufferString.missing.toString
      )
        .mergeNonMissing(
          BufferString("1", BufferString.missing.toString, "1", "0", "1")
        )
        .toSeq,
      Seq("0", BufferString.missing.toString, "0", "1", "1")
    )
  }

  test("isMissing") {
    assert(BufferString("0").isMissing(0) == false)
    assert(BufferString("").isMissing(0) == false)
    assert(BufferString(BufferString.missing.toString).isMissing(0) == true)
  }
  test("elementwise eq") {
    assert(
      BufferString("0", "1")
        .elementwise_eq(
          BufferString(
            Array[CharSequence](
              CharBuffer.allocate(1).put('0').rewind(),
              CharBuffer.allocate(1).put('1').rewind()
            )
          )
        )
        .toSeq == List(1, 1)
    )
  }

  test("statistic mightEq") {
    1 to 1000 foreach{ _ =>
      val ar = 1 to 1000 map (_ => scala.util.Random.alphanumeric.take(10).mkString) toList
      val b = BufferString(ar:_*)
      val st = b.makeStatistic()
      assert(ar.forall(l => st.mightEq(l)))
    }
  }
  test("statistic mightLt") {
    1 to 1000 foreach{ _ =>
      val ar = 1 to 1000 map (_ => scala.util.Random.alphanumeric.take(10).mkString) toList
      val b = BufferString(ar:_*)
      val st = b.makeStatistic()
      assert(ar.forall(l => st.mightLt(l+"a")))
    }
  }
  test("statistic mightLtEq") {
    1 to 1000 foreach{ _ =>
      val ar = 1 to 1000 map (_ => scala.util.Random.alphanumeric.take(10).mkString) toList
      val b = BufferString(ar:_*)
      val st = b.makeStatistic()
      assert(ar.forall(l => st.mightLtEq(l)))
    }
  }
  test("statistic mightGtEq") {
    1 to 1000 foreach{ _ =>
      val ar = 1 to 1000 map (_ => scala.util.Random.alphanumeric.take(10).mkString) toList
      val b = BufferString(ar:_*)
      val st = b.makeStatistic()
      assert(ar.forall(l => st.mightGtEq(l)))
    }
  }
  test("statistic mightGt") {
    1 to 1000 foreach{ _ =>
      val ar = 1 to 1000 map (_ => scala.util.Random.alphanumeric.take(10).mkString) toList
      val b = BufferString(ar:_*)
      val st = b.makeStatistic()
      assert(ar.forall(l => st.mightGt(l.dropRight(1))))
    }
  }

}

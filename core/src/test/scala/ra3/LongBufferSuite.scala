package ra3

import cats.effect.unsafe.implicits.global

class LongBufferSuite extends munit.FunSuite with WithTempTaskSystem {
  test("makeStatistic") {
    val s = Seq(0L, 1L, 2L, 3L, Long.MinValue, -1L)
    val st = BufferLong(s*).makeStatistic()
    assertEquals(st.hasMissing, true)
    assertEquals(st.nonMissingMinMax, Some(-1L -> 3L))
    assertEquals(st.lowCardinalityNonMissingSet, Some(Set(0L, 1L, 2L, 3L, -1L)))

  }
  test("makeStatistic long") {
    val s = Seq(0 until 256*).map(_.toLong)
    val st = BufferLong(s*).makeStatistic()
    assertEquals(st.hasMissing, false)
    assertEquals(st.nonMissingMinMax, Some(0L -> 255L))
    assertEquals(st.lowCardinalityNonMissingSet, None)

  }

  test("toSegment") {
    withTempTaskSystem { implicit tsc =>
      val s = Seq(0L, 1L, 2L, 3L, Long.MinValue, -1L)
      val segment = BufferLong(s.toArray)
        .toSegment(LogicalPath("toSegmentTest", None, 0, 0))
        .unsafeRunSync()
      assertEquals(segment.buffer.unsafeRunSync().toSeq, s)
      assertEquals(segment.numElems, 6)
      assertEquals(segment.nonMissingMinMax.get, (-1L, 3L))
      assertEquals(segment.statistic.hasMissing, true)
    }
  }
  test("findInequalityVsHead") {
    val b1 = BufferLong(Array(0, 1, 2, 3, 4, 4, 5, 6).map(_.toLong))
    val b2 = BufferLong(Array(Long.MinValue))
    val b3 = BufferLong(Array(0L))
    assertEquals(b1.findInequalityVsHead(b2, true).toSeq, Nil)
    assertEquals(b1.findInequalityVsHead(b3, true).toSeq, List(0))
    assertEquals(
      b1.findInequalityVsHead(b3, false).toSeq,
      List(0, 1, 2, 3, 4, 5, 6, 7)
    )
  }
  test("cdf") {
    val b1 = BufferLong(Array(0, 1, 2, 3, 4, 4, 5, 6).map(_.toLong))
    assertEquals(ColumnTag.I64.cdf(b1, 2)._1.values.toSeq, List(0L, 6L))
    assertEquals(ColumnTag.I64.cdf(b1, 2)._2.values.toSeq, List(0d, 1d))
    assertEquals(ColumnTag.I64.cdf(b1, 4)._1.values.toSeq, List(0L, 2L, 4L, 6L))
    assertEquals(
      ColumnTag.I64.cdf(b1, 4)._2.values.toSeq,
      List(0d, 1d / 3d, 2d / 3d, 1d)
    )
  }
  test("length") {
    val b1 = BufferLong(Array(0L, 1, 2, 3, 4, 4, 5, 6))
    assertEquals(b1.length, 8)
  }
  test("groups") {
    val b1 = BufferInt(Array(0, 1, 2, 3, 4, 4, 5, 6).reverse)
    assertEquals(b1.groups.map.toSeq, List(0, 1, 2, 2, 3, 4, 5, 6))
    assertEquals(b1.groups.groupSizes.toSeq, List(1, 1, 2, 1, 1, 1, 1))
    assertEquals(b1.groups.numGroups, 7)
  }
  test("take") {
    val b1 = BufferLong(Array(99, 1, 2, Long.MinValue, 4, 4, 5, 6))
    assertEquals(
      b1.take(BufferInt(-1, 0, 3, 0, -1)).toSeq,
      Seq(Long.MinValue, 99, Long.MinValue, 99, Long.MinValue)
    )
  }
  test("positiveLocations") {
    val b1 = BufferLong(Array(Long.MinValue, 1, 2, -4, 4, 4, 5, 6))
    assertEquals(b1.positiveLocations.toSeq, Seq(1, 2, 4, 5, 6, 7))
  }
  test("outer join") {
    val (a, b) = BufferLong(0, Long.MinValue, 1, -99).computeJoinIndexes(
      BufferLong(0, 0, Long.MinValue, Long.MinValue, 1, 1, 99),
      "outer"
    )
    assertEquals(a.get.toSeq, Seq(0, 0, 1, 2, 2, 3, -1, -1, -1))
    assertEquals(b.get.toSeq, Seq(0, 1, -1, 4, 5, -1, 2, 3, 6))
  }

  test("mergeNonMissing") {
    assertEquals(
      BufferLong(0, Long.MinValue, 0, 1, Long.MinValue)
        .mergeNonMissing(BufferLong(1, Long.MinValue, 1, 0, 1))
        .toSeq,
      Seq(0, Long.MinValue, 0, 1, 1)
    )
  }

  test("isMissing") {
    assert(BufferLong(0).isMissing(0) == false)
    assert(BufferLong(Long.MinValue).isMissing(0) == true)
  }

  test("sum groups") {
    assertEquals(
      BufferLong(0, 0, 1, 1, 2, 2, Long.MinValue)
        .sumGroups(BufferInt(0, 1, 0, 1, 0, 1, 1), 2)
        .toSeq,
      Seq(3L, 3L)
    )
  }

  test("statistic mightEq") {
    1 to 1000 foreach { _ =>
      val ar = org.saddle.array.randLong(10000)
      val b = BufferLong(ar)
      val st = b.makeStatistic()
      assert(ar.forall(l => st.mightEq(l)))
    }
  }
  test("statistic mightLt") {
    1 to 1000 foreach { _ =>
      val ar = org.saddle.array.randLong(10000)
      val b = BufferLong(ar)
      val st = b.makeStatistic()
      assert(ar.forall(l => st.mightLt(l + 1)))
    }
  }
  test("statistic mightLtEq") {
    1 to 1000 foreach { _ =>
      val ar = org.saddle.array.randLong(10000)
      val b = BufferLong(ar)
      val st = b.makeStatistic()
      assert(ar.forall(l => st.mightLtEq(l)))
    }
  }
  test("statistic mightGtEq") {
    1 to 1000 foreach { _ =>
      val ar = org.saddle.array.randLong(10000)
      val b = BufferLong(ar)
      val st = b.makeStatistic()
      assert(ar.forall(l => st.mightGtEq(l)))
    }
  }
  test("statistic mightGt") {
    1 to 1000 foreach { _ =>
      val ar = org.saddle.array.randLong(10000)
      val b = BufferLong(ar)
      val st = b.makeStatistic()
      assert(ar.forall(l => st.mightGt(l - 1)))
    }
  }

}

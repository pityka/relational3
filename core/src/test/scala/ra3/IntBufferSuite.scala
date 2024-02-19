package ra3

import cats.effect.unsafe.implicits.global

class IntBufferSuite extends munit.FunSuite with WithTempTaskSystem {
  test("makeStatistic") {
      val s = Seq(0, 1, 2, 3, Int.MinValue, -1)
      val st = BufferInt(s: _*).asInstanceOf[BufferIntInArray].makeStatistic()
      assertEquals(st.hasMissing, true)
      assertEquals(st.nonMissingMinMax, Some(-1 -> 3))
      assertEquals(st.lowCardinalityNonMissingSet, Some(Set(0, 1, 2, 3, -1)))

  }
  test("makeStatistic long") {
      val s = Seq(0 until 256:_*)
      val st = BufferInt(s: _*).asInstanceOf[BufferIntInArray].makeStatistic()
      assertEquals(st.hasMissing, false)
      assertEquals(st.nonMissingMinMax, Some(0 -> 255))
      assertEquals(st.lowCardinalityNonMissingSet, None)

  }
  test("toSegment") {
    withTempTaskSystem { implicit tsc =>
      val s = Seq(0, 1, 2, 3, Int.MinValue, -1)
      println(BufferInt(s: _*))
      val segment = BufferInt(s: _*)
        .toSegment(LogicalPath("toSegmentTest", None, 0, 0))
        .unsafeRunSync()
      assertEquals(segment.buffer.unsafeRunSync().toSeq, s)
      assertEquals(segment.numElems, 6)
      assertEquals(segment.nonMissingMinMax.get, (-1, 3))
      assertEquals(segment.statistic.hasMissing, true)
    }
  }
  test("toSegment empty") {
    withTempTaskSystem { implicit tsc =>
      val s = Seq.empty[Int]
      val segment = BufferInt(s: _*)
        .toSegment(LogicalPath("toSegmentTest2", None, 0, 0))
        .unsafeRunSync()
      assertEquals(BufferInt(s: _*), BufferIntConstant(Int.MinValue, 0))
      assertEquals(segment.buffer.unsafeRunSync().toSeq, s)
      assertEquals(segment.numElems, 0)
      assertEquals(segment.nonMissingMinMax, None)
      assert(segment.sf.isEmpty)
    }
  }
  test("toSegment constant") {
    withTempTaskSystem { implicit tsc =>
      val s = Seq(1, 1, 1, 1, 1, 1)
      val segment = BufferInt(s: _*)
        .toSegment(LogicalPath("toSegmentTest3", None, 0, 0))
        .unsafeRunSync()
      assertEquals(BufferInt(s: _*), BufferIntConstant(1, 6))
      assertEquals(segment.buffer.unsafeRunSync().toSeq, s)
      assertEquals(segment.numElems, 6)
      assertEquals(segment.nonMissingMinMax.get, (1, 1))
      assert(segment.sf.isEmpty)
    }
  }
  test("findInequalityVsHead") {
    val b1 = BufferInt(Array(0, 1, 2, 3, 4, 4, 5, 6))
    val b2 = BufferInt(Array(Int.MinValue))
    val b3 = BufferInt(Array(0))
    assertEquals(b1.findInequalityVsHead(b2, true).toSeq, Nil)
    assertEquals(b1.findInequalityVsHead(b3, true).toSeq, List(0))
    assertEquals(
      b1.findInequalityVsHead(b3, false).toSeq,
      List(0, 1, 2, 3, 4, 5, 6, 7)
    )
  }
  test("cdf") {
    val b1 = BufferInt(Array(0, 1, 2, 3, 4, 4, 5, 6))
    assertEquals(b1.cdf(2)._1.toSeq, List(0, 6))
    assertEquals(b1.cdf(2)._2.toSeq, List(0d, 1d))
    assertEquals(b1.cdf(4)._1.toSeq, List(0, 2, 4, 6))
    assertEquals(b1.cdf(4)._2.toSeq, List(0d, 1d / 3d, 2d / 3d, 1d))
  }
  test("length") {
    val b1 = BufferInt(Array(0, 1, 2, 3, 4, 4, 5, 6))
    assertEquals(b1.length, 8)
  }
  test("groups") {
    val b1 = BufferInt(Array(0, 1, 2, 3, 4, 4, 5, 6).reverse)
    assertEquals(b1.groups.map.toSeq, List(0, 1, 2, 2, 3, 4, 5, 6))
    assertEquals(b1.groups.groupSizes.toSeq, List(1, 1, 2, 1, 1, 1, 1))
    assertEquals(b1.groups.numGroups, 7)
  }
  test("take") {
    val b1 = BufferInt(Array(99, 1, 2, Int.MinValue, 4, 4, 5, 6))
    assertEquals(
      b1.take(BufferInt(-1, 0, 3, 0, -1)).toSeq,
      Seq(Int.MinValue, 99, Int.MinValue, 99, Int.MinValue)
    )
  }
  test("positiveLocations") {
    val b1 = BufferInt(Array(Int.MinValue, 1, 2, -4, 4, 4, 5, 6))
    assertEquals(b1.positiveLocations.toSeq, Seq(1, 2, 4, 5, 6, 7))
  }
  test("outer join") {
    val (a, b) = BufferInt(0, Int.MinValue, 1, -99).computeJoinIndexes(
      BufferInt(0, 0, Int.MinValue, Int.MinValue, 1, 1, 99),
      "outer"
    )
    
    assertEquals(a.get.toSeq, Seq(0, 0, 1, 2, 2, 3, -1, -1 ,-1))
    assertEquals(b.get.toSeq, Seq(0, 1, -1, 4, 5, -1, 2,3,6))
  }

  test("mergeNonMissing") {
    assertEquals(
      BufferInt(0, Int.MinValue, 0, 1, Int.MinValue)
        .mergeNonMissing(BufferInt(1, Int.MinValue, 1, 0, 1))
        .toSeq,
      Seq(0, Int.MinValue, 0, 1, 1)
    )
  }

  test("isMissing") {
    assert(BufferInt(0).isMissing(0) == false)
    assert(BufferInt(Int.MinValue).isMissing(0) == true)
  }

  test("sum groups") {
    assertEquals(
      BufferInt(0, 0, 1, 1, 2, 2, Int.MinValue)
        .sumGroups(BufferInt(0, 1, 0, 1, 0, 1, 1), 2)
        .toSeq,
      Seq(3, 3)
    )
  }
  test("broadcast") {
    assertEquals(
      BufferIntInArray(Array(1)).broadcast(3),
      BufferIntConstant(1, 3)
    )
  }

}

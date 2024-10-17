package ra3

import cats.effect.unsafe.implicits.global

class ConstantIntBufferSuite extends munit.FunSuite with WithTempTaskSystem {
  test("makeStatistic") {
    val st =
      BufferInt.constant(0, 6).asInstanceOf[BufferIntConstant].makeStatistic()
    assertEquals(st.hasMissing, false)
    assertEquals(st.nonMissingMinMax, Some(0 -> 0))
    assertEquals(st.lowCardinalityNonMissingSet, Some(Set(0)))

  }

  test("toSegment") {
    withTempTaskSystem { implicit tsc =>

      val segment = ColumnTag.I32
        .toSegment(
          BufferInt.constant(0, 6),
          LogicalPath("toSegmentTest", None, 0, 0)
        )
        .unsafeRunSync()
      assertEquals(segment.buffer.unsafeRunSync().toSeq, Seq(0, 0, 0, 0, 0, 0))
      assertEquals(segment.numElems, 6)
      assertEquals(segment.nonMissingMinMax.get, (0, 0))
      assertEquals(segment.statistic.hasMissing, false)
    }
  }
  test("toSegment empty") {
    withTempTaskSystem { implicit tsc =>
      val segment = ColumnTag.I32
        .toSegment(
          BufferInt.constant(0, 0),
          LogicalPath("toSegmentTest2", None, 0, 0)
        )
        .unsafeRunSync()
      assertEquals(segment.buffer.unsafeRunSync().toSeq, Seq.empty)
      assertEquals(segment.numElems, 0)
      assertEquals(segment.nonMissingMinMax, None)
      assert(segment.sf.isEmpty)
    }
  }
  test("toSegment constant") {
    withTempTaskSystem { implicit tsc =>
      val s = Seq(1, 1, 1, 1, 1, 1)
      val segment = ColumnTag.I32
        .toSegment(BufferInt(s*), LogicalPath("toSegmentTest3", None, 0, 0))
        .unsafeRunSync()
      assertEquals(BufferInt(s*), BufferIntConstant(1, 6))
      assertEquals(segment.buffer.unsafeRunSync().toSeq, s)
      assertEquals(segment.numElems, 6)
      assertEquals(segment.nonMissingMinMax.get, (1, 1))
      assert(segment.sf.isEmpty)
    }
  }
  test("findInequalityVsHead") {
    val b1 = BufferInt.constant(0, 3)
    val b2 = BufferInt(Array(Int.MinValue))
    val b3 = BufferInt(Array(0))
    assertEquals(ColumnTag.I32.findInequalityVsHead(b1, b2, true).toSeq, Nil)
    assertEquals(
      ColumnTag.I32.findInequalityVsHead(b1, b3, true).toSeq,
      List(0, 1, 2)
    )
    assertEquals(
      ColumnTag.I32.findInequalityVsHead(b1, b3, false).toSeq,
      List(0, 1, 2)
    )
  }
  test("cdf") {
    val b1 = BufferInt.constant(0, 8)
    assertEquals(ColumnTag.I32.cdf(b1, 2)._1.toSeq, List(0, 0))
    assertEquals(ColumnTag.I32.cdf(b1, 2)._2.toSeq, List(0d, 1d))
    assertEquals(ColumnTag.I32.cdf(b1, 4)._1.toSeq, List(0, 0, 0, 0))
    assertEquals(
      ColumnTag.I32.cdf(b1, 4)._2.toSeq,
      List(0d, 1d / 3d, 2d / 3d, 1d)
    )
  }
  test("length") {
    val b1 = BufferInt.constant(0, 8)
    assertEquals(b1.length, 8)
  }
  test("groups") {
    val b1 = BufferInt.constant(0, 4)
    assertEquals(b1.groups.map.toSeq, List(0, 0, 0, 0))
    assertEquals(b1.groups.groupSizes.toSeq, List(4))
    assertEquals(b1.groups.numGroups, 1)
  }
  test("take") {
    val b1 = BufferInt.constant(0, 4)
    assertEquals(
      ColumnTag.I32.take(b1, BufferInt(-1, 0, 3, 0, -1)).toSeq,
      Seq(Int.MinValue, 0, 0, 0, Int.MinValue)
    )
  }
  test("positiveLocations") {
    val b1 = BufferInt.constant(0, 4)
    assertEquals(b1.positiveLocations.toSeq, Seq())
    assertEquals(
      BufferInt.constant(1, 4).positiveLocations.toSeq,
      Seq(0, 1, 2, 3)
    )
  }
  test("outer join") {
    val (a, b) = ColumnTag.I32.computeJoinIndexes(
      BufferInt.constant(0, 3),
      BufferInt.constant(0, 4),
      "outer"
    )

    assertEquals(a.get.toSeq, Seq(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2))
    assertEquals(b.get.toSeq, Seq(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3))
  }

  test("mergeNonMissing") {
    assertEquals(
      ColumnTag.I32
        .mergeNonMissing(
          BufferInt.constant(Int.MinValue, 3),
          BufferInt.constant(0, 3)
        )
        .toSeq,
      Seq(0, 0, 0)
    )
  }

  test("isMissing") {
    assert(BufferInt.constant(0, 3).isMissing(0) == false)
    assert(BufferInt.constant(0, 3).isMissing(1) == false)
    assert(BufferInt.constant(0, 3).isMissing(2) == false)
    assert(BufferInt.constant(Int.MinValue, 3).isMissing(0) == true)
    assert(BufferInt.constant(Int.MinValue, 3).isMissing(1) == true)
    assert(BufferInt.constant(Int.MinValue, 3).isMissing(2) == true)
    assert(BufferInt.constant(Int.MinValue, 3).isMissing(3) == true)

  }

  test("sum groups") {
    assertEquals(
      BufferInt
        .constant(1, 3)
        .sumGroups(BufferInt(0, 1, 0), 2)
        .toSeq,
      Seq(2, 1)
    )
  }
  test("broadcast") {
    assertEquals(
      BufferInt.constant(1, 1).broadcast(3),
      BufferIntConstant(1, 3)
    )
  }

}

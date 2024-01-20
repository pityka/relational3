package ra3

import cats.effect.unsafe.implicits.global

class DoubleBufferSuite extends munit.FunSuite with WithTempTaskSystem {
  def assertEqualDoubleSeq(a: Seq[Double],b:Seq[Double]) = assertEquals(a.map(_.toString),b.map(_.toString))
  test("toSegment") {
    withTempTaskSystem { implicit tsc =>
      val s = Seq(0d, 1d, 2d, 3d, Double.NaN, -1d)
      val segment = BufferDouble(s: _*)
        .toSegment(LogicalPath("toSegmentTest", None, 0, 0))
        .unsafeRunSync()
      assertEqualDoubleSeq(segment.buffer.unsafeRunSync().toSeq, s)
      assertEquals(segment.numElems, 6)
      assertEquals(segment.minMax.get.toString, (Double.NaN, 3d).toString)
    }
  }
  test("findInequalityVsHead") {
    val b1 = BufferDouble(Array(0, 1, 2, 3, 4, 4, 5, 6).map(_.toDouble))
    val b2 = BufferDouble(Array(Double.NaN))
    val b3 = BufferDouble(Array(0d))
    assertEquals(b1.findInequalityVsHead(b2, true).toSeq, Nil)
    assertEquals(b1.findInequalityVsHead(b3, true).toSeq, List(0))
    assertEquals(
      b1.findInequalityVsHead(b3, false).toSeq,
      List(0, 1, 2, 3, 4, 5, 6, 7)
    )
  }
  test("cdf") {
    val b1 = BufferDouble(Array(0, 1, 2, 3, 4, 4, 5, 6).map(_.toDouble))
    assertEquals(b1.cdf(2)._1.values.toSeq, List(0d, 6d))
    assertEquals(b1.cdf(2)._2.values.toSeq, List(0d, 1d))
    assertEquals(b1.cdf(4)._1.values.toSeq, List(0d, 2d, 4d, 6d))
    assertEquals(b1.cdf(4)._2.values.toSeq, List(0d, 1d / 3d, 2d / 3d, 1d))
  }
  test("length") {
    val b1 = BufferDouble(Array(0, 1, 2, 3, 4, 4, 5, 6).map(_.toDouble))
    assertEquals(b1.length, 8)
  }
  test("groups") {
    val b1 = BufferDouble(Array(0, 1, 2, 3, 4, 4, 5, 6).reverse.map(_.toDouble))
    assertEquals(b1.groups.map.toSeq, List(0, 1, 2, 2, 3, 4, 5, 6))
    assertEquals(b1.groups.groupSizes.toSeq, List(1, 1, 2, 1, 1, 1, 1))
    assertEquals(b1.groups.numGroups, 7)
  }
  test("take") {
    val b1 = BufferDouble(Array(99d, 1d, 2d, Double.NaN, 4d, 4d, 5d, 6d))
    assertEqualDoubleSeq(
      b1.take(BufferInt(-1, 0, 3, 0, -1)).toSeq,
      Seq(Double.NaN, 99d, Double.NaN, 99d, Double.NaN)
    )
  }
  test("positiveLocations") {
    val b1 = BufferDouble(Array(Double.NaN, 1d, 2d, -4d, 4d, 4d, 5d, 6d))
    assertEquals(b1.positiveLocations.toSeq, Seq(1, 2, 4, 5, 6, 7))
  }
  test("outer join") {
    val (a, b) = BufferDouble(0d, Double.NaN, 1d, -99d).computeJoinIndexes(
      BufferDouble(0d, 0d, Double.NaN, Double.NaN, 1d, 1d, 99d),
      "outer"
    )
    assertEquals(a.get.toSeq, Seq(0, 0, 1, 1, 2, 2, 3, -1))
    assertEquals(b.get.toSeq, Seq(0, 1, 2, 3, 4, 5, -1, 6))
  }

  test("mergeNonMissing") {
    assertEqualDoubleSeq(
      BufferDouble(0d, Double.NaN, 0d, 1d, Double.NaN)
        .mergeNonMissing(BufferDouble(1d, Double.NaN, 1d, 0d, 1d))
        .toSeq,
      Seq(0, Double.NaN, 0d, 1d, 1d)
    )
  }

  test("isMissing") {
    assert(BufferDouble(0d).isMissing(0) == false)
    assert(BufferDouble(Double.NaN).isMissing(0) == true)
  }

  test("sum groups") {
    assertEquals(
      BufferDouble(0d, 0d, 1d, 1d, 2d, 2d,Double.NaN)
        .sumGroups(BufferInt(0, 1, 0, 1, 0, 1,1), 2)
        .toSeq,
      Seq(3d, 3d)
    )
  }

}

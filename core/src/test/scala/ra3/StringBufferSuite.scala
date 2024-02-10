package ra3

import cats.effect.unsafe.implicits.global
import java.nio.CharBuffer

class StringBufferSuite extends munit.FunSuite with WithTempTaskSystem {
  test("toSegment") {
    withTempTaskSystem { implicit tsc =>
      val s: Seq[CharSequence] =
        Seq("0", "1", "2", "3", s"${Char.MinValue}", ".", "")
      val segment = BufferString(s.toArray)
        .toSegment(LogicalPath("toSegmentTest", None, 0, 0))
        .unsafeRunSync()
      assertEquals(segment.buffer.unsafeRunSync().toSeq.map(_.toString), s)
      assertEquals(segment.numElems, 7)
      assertEquals(segment.minMax.get, (s"${Char.MinValue}", "3"))
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
    assertEquals(a.get.toSeq, Seq(0, 0, 1, 1, 2, 2, 3, -1))
    assertEquals(b.get.toSeq, Seq(0, 1, 2, 3, 4, 5, -1, 6))
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
    assert(BufferString(null.asInstanceOf[String]).isMissing(0) == false)
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

}

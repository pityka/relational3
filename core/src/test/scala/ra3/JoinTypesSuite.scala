package ra3

class JoinTypesSuite extends munit.FunSuite {

  // Inner join
  test("int inner join") {
    val (a, b) = BufferIntInArray(Array(1, 2, 3)).computeJoinIndexes(
      BufferIntInArray(Array(2, 3, 4)),
      "inner"
    )
    assertEquals(a.get.toSeq, Seq(1, 2))
    assertEquals(b.get.toSeq, Seq(0, 1))
  }
  test("int inner join with missing") {
    val (a, b) = BufferIntInArray(Array(1, Int.MinValue, 3)).computeJoinIndexes(
      BufferIntInArray(Array(Int.MinValue, 3)),
      "inner"
    )
    // missing values should not join
    assertEquals(a.get.toSeq, Seq(2))
    assertEquals(b.get.toSeq, Seq(1))
  }

  // Left join - lTake is None (identity), rTake has -1 for unmatched
  test("int left join") {
    val (a, b) = BufferIntInArray(Array(1, 2, 3)).computeJoinIndexes(
      BufferIntInArray(Array(2, 4)),
      "left"
    )
    assertEquals(a, None) // identity: take all left rows as-is
    assertEquals(b.get.toSeq, Seq(-1, 0, -1))
  }

  // Right join - rTake is None (identity), lTake has -1 for unmatched
  test("int right join") {
    val (a, b) = BufferIntInArray(Array(1, 2)).computeJoinIndexes(
      BufferIntInArray(Array(2, 3, 4)),
      "right"
    )
    assertEquals(a.get.toSeq, Seq(1, -1, -1))
    assertEquals(b, None) // identity: take all right rows as-is
  }

  // Outer join (already tested elsewhere, but with duplicates)
  test("int outer join with duplicates") {
    val (a, b) = BufferIntInArray(Array(1, 1, 2)).computeJoinIndexes(
      BufferIntInArray(Array(1, 3)),
      "outer"
    )
    // 1 matches: (0,0), (1,0); 2 unmatched left; 3 unmatched right
    assertEquals(a.get.toSeq, Seq(0, 1, 2, -1))
    assertEquals(b.get.toSeq, Seq(0, 0, -1, 1))
  }

  // Empty table joins
  test("inner join with empty left") {
    val (a, b) = BufferIntInArray(Array.empty[Int]).computeJoinIndexes(
      BufferIntInArray(Array(1, 2)),
      "inner"
    )
    assertEquals(a.get.toSeq, Seq.empty)
    assertEquals(b.get.toSeq, Seq.empty)
  }
  test("left join with empty right") {
    val (a, b) = BufferIntInArray(Array(1, 2)).computeJoinIndexes(
      BufferIntInArray(Array.empty[Int]),
      "left"
    )
    assertEquals(a, None) // identity
    assertEquals(b.get.toSeq, Seq(-1, -1))
  }

  // Long inner join
  test("long inner join") {
    val (a, b) = BufferLong(1L, 2L, 3L).computeJoinIndexes(
      BufferLong(2L, 3L, 4L),
      "inner"
    )
    assertEquals(a.get.toSeq, Seq(1, 2))
    assertEquals(b.get.toSeq, Seq(0, 1))
  }
  test("long inner join with missing") {
    val (a, b) = BufferLong(1L, Long.MinValue).computeJoinIndexes(
      BufferLong(Long.MinValue, 1L),
      "inner"
    )
    assertEquals(a.get.toSeq, Seq(0))
    assertEquals(b.get.toSeq, Seq(1))
  }

  // String inner join
  test("string inner join") {
    val (a, b) = BufferString("a", "b", "c").computeJoinIndexes(
      BufferString("b", "c", "d"),
      "inner"
    )
    assertEquals(a.get.toSeq, Seq(1, 2))
    assertEquals(b.get.toSeq, Seq(0, 1))
  }

  // All-missing join
  test("int inner join all missing") {
    val (a, b) = BufferIntInArray(Array(Int.MinValue, Int.MinValue)).computeJoinIndexes(
      BufferIntInArray(Array(Int.MinValue, Int.MinValue)),
      "inner"
    )
    assertEquals(a.get.toSeq, Seq.empty)
    assertEquals(b.get.toSeq, Seq.empty)
  }
}

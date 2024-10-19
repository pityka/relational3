package ra3.lang
private[ra3] object PrepartitionBy {
  def apply(
      a: Expr.DelayedIdent[?]
  ) =
    PrepartitionBuilderSyntax(a, Vector.empty, None, None, None)
}

/** Builder pattern for group by clause. Exit the builder with the partial or
  * the all method.
  */
case class PrepartitionBuilderSyntax(
    private val first: Expr.DelayedIdent[?],
    private val others: Vector[
      (Expr.DelayedIdent[?])
    ],
    private val partitionBase: Option[Int],
    private val partitionLimit: Option[Int],
    private val maxItemsToBufferAtOnce: Option[Int]
) { self =>

  def by(n: Expr.DelayedIdent[?]) = {
    require(first.name.table == n.name.table)
    copy(others = others :+ n)
  }

  def withPartitionBase(num: Int) = copy(partitionBase = Some(num))
  def withPartitionLimit(num: Int) = copy(partitionLimit = Some(num))
  def withMaxSegmentsBufferingAtOnce(num: Int) =
    copy(maxItemsToBufferAtOnce = Some(num))

  /** Total reduction. Applies the group wise program to each group.
    *
    * @param prg
    *   group wise program
    * @return
    */
  def done =
    ra3.tablelang.TableExpr.Prepartition(
      first,
      others,
      partitionBase.getOrElse(128),
      partitionLimit.getOrElse(10_000_000),
      maxItemsToBufferAtOnce.getOrElse(1000_000)
    )

}

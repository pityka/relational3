package ra3.lang
private[ra3] object GroupBy {
  def apply[T, K <: ReturnValue[T]](
      a: Expr.DelayedIdent[?],
      prg: ra3.lang.Expr[K]
  ) =
    GroupBuilderSyntax[T, K](a, Vector.empty, prg, None, None, None)
}

/** Builder pattern for group by clause. Exit the builder with the partial or
  * the all method.
  */
case class GroupBuilderSyntax[T, K <: ReturnValue[T]](
    private val first: Expr.DelayedIdent[?],
    private val others: Vector[
      (Expr.DelayedIdent[?])
    ],
    val prg: ra3.lang.Expr[K],
    private val partitionBase: Option[Int],
    private val partitionLimit: Option[Int],
    private val maxSegmentsToBufferAtOnce: Option[Int]
) { self =>

  def by(n: Expr.DelayedIdent[?]) = {
    require(first.name.table == n.name.table)
    copy(others = others :+ n)
  }

  def withPartitionBase(num: Int) = copy(partitionBase = Some(num))
  def withPartitionLimit(num: Int) = copy(partitionLimit = Some(num))
  def withMaxSegmentsBufferingAtOnce(num: Int) =
    copy(maxSegmentsToBufferAtOnce = Some(num))
  def all =
    ra3.tablelang.TableExpr.GroupThenReduce(
      first,
      others,
      prg,
      // prg.getOrElse(ra3.select(ra3.star)),
      partitionBase.getOrElse(128),
      partitionLimit.getOrElse(10_000_000),
      maxSegmentsToBufferAtOnce.getOrElse(10)
    )
  def partial =
    ra3.tablelang.TableExpr.GroupPartialThenReduce(
      first,
      others,
      prg
      // prg.getOrElse(ra3.select(ra3.star))
    )
  def count = ra3.tablelang.TableExpr.GroupThenCount(
    first,
    others,
    // prg.getOrElse(ra3.select(ra3.star)),
    prg,
    partitionBase.getOrElse(128),
    partitionLimit.getOrElse(10_000_000),
    maxSegmentsToBufferAtOnce.getOrElse(10)
  )

}

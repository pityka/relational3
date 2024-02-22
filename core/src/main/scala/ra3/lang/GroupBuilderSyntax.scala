package ra3.lang
object GroupBy {
  def apply(a: Expr.DelayedIdent) =
    GroupBuilderSyntax(a, Vector.empty, None, None, None, None)
}
private[ra3] case class GroupBuilderSyntax(
    private val first: Expr.DelayedIdent,
    private val others: Vector[
      (Expr.DelayedIdent)
    ],
    private val prg: Option[ra3.lang.Query],
    private val partitionBase: Option[Int],
    private val partitionLimit: Option[Int],
    private val maxSegmentsToBufferAtOnce: Option[Int]
) {

  def by(n: Expr.DelayedIdent) = {
    require(first.name.table == n.name.table)
    copy(others = others :+ n)
  }

  def withPartitionBase(num: Int) = copy(partitionBase = Some(num))
  def withPartitionLimit(num: Int) = copy(partitionLimit = Some(num))
  def withMaxSegmentsBufferingAtOnce(num: Int) =
    copy(maxSegmentsToBufferAtOnce = Some(num))
  def apply(q: ra3.lang.Query) = copy(prg = Some(q))
  def all =
    ra3.tablelang.TableExpr.GroupThenReduce(
      first,
      others,
      prg.getOrElse(ra3.select(ra3.star)),
      partitionBase.getOrElse(128),
      partitionLimit.getOrElse(10_000_000),
      maxSegmentsToBufferAtOnce.getOrElse(10)
    )
  def partial =
    ra3.tablelang.TableExpr.GroupPartialThenReduce(
      first,
      others,
      prg.getOrElse(ra3.select(ra3.star))
    )
  def count = ra3.tablelang.TableExpr.GroupThenCount(
    first,
    others,
    prg.getOrElse(ra3.select(ra3.star)),
    partitionBase.getOrElse(128),
    partitionLimit.getOrElse(10_000_000),
    maxSegmentsToBufferAtOnce.getOrElse(10)
  )

}

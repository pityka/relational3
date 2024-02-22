package ra3.lang
import ra3.tablelang.TableExpr

object Join {
  def apply(a: Expr.DelayedIdent) =
    JoinBuilderSyntax(a, Vector.empty, None, None, None, None)
}

private[ra3] case class JoinBuilderSyntax(
    private val first: Expr.DelayedIdent,
    private val others: Vector[
      (Expr.DelayedIdent, String, ra3.tablelang.Key)
    ],
    private val prg: Option[ra3.lang.Query],
    private val partitionBase: Option[Int],
    private val partitionLimit: Option[Int],
    private val maxSegmentsToBufferAtOnce: Option[Int]
) {
  def withMaxSegmentsBufferingAtOnce(num: Int) =
    copy(maxSegmentsToBufferAtOnce = Some(num))

  def inner(ref: Expr.DelayedIdent, other: TableExpr.Ident) = {
    copy(
      others = others.appended(
        (ref, "inner", other.key)
      )
    )
  }
  def inner(ref: Expr.DelayedIdent) = {
    copy(
      others = others.appended(
        (ref, "inner", first.name.table)
      )
    )
  }
  def outer(ref: Expr.DelayedIdent, other: TableExpr.Ident) = {
    copy(
      others = others.appended(
        (ref, "outer", other.key)
      )
    )
  }
  def outer(ref: Expr.DelayedIdent) = {
    copy(
      others = others.appended(
        (ref, "outer", first.name.table)
      )
    )
  }
  def left(ref: Expr.DelayedIdent, other: TableExpr.Ident) = {
    copy(
      others = others.appended(
        (ref, "left", other.key)
      )
    )
  }
  def left(ref: Expr.DelayedIdent) = {
    copy(
      others = others.appended(
        (ref, "left", first.name.table)
      )
    )
  }
  def right(ref: Expr.DelayedIdent, other: TableExpr.Ident) = {
    copy(
      others = others.appended(
        (ref, "right", other.key)
      )
    )
  }
  def right(ref: Expr.DelayedIdent) = {
    copy(
      others = others.appended(
        (ref, "right", first.name.table)
      )
    )
  }
  def withPartitionBase(num: Int) = copy(partitionBase = Some(num))
  def withPartitionLimit(num: Int) = copy(partitionLimit = Some(num))
  def elementwise(q: ra3.lang.Query) = copy(prg = Some(q)).done
  def done = {
    ra3.tablelang.TableExpr.Join(
      first,
      others,
      partitionBase.getOrElse(128),
      partitionLimit.getOrElse(10_000_000),
      maxSegmentsToBufferAtOnce.getOrElse(10),
      prg.getOrElse(ra3.select(ra3.star))
    )
  }
}

package ra3.lang
import ra3.tablelang.TableExpr

private[ra3] object Join {
  def apply[J,K,R<:ReturnValue[K]](a: Expr.DelayedIdent[J], prg: ra3.lang.Expr[R]) =
    JoinBuilderSyntax[J,K,R](a, Vector.empty, prg, None, None, None)
}

/** Builder pattern for joins. Exit the builder with the done method or elementwise method */
case class JoinBuilderSyntax[J,K,R<:ReturnValue[K]](
    private val first: Expr.DelayedIdent[J],
    private val others: Vector[
      (Expr.DelayedIdent[J], String, ra3.tablelang.Key)
    ],
    val prg: ra3.lang.Expr[R],
    private val partitionBase: Option[Int],
    private val partitionLimit: Option[Int],
    private val maxSegmentsToBufferAtOnce: Option[Int]
) { self =>
  def withMaxSegmentsBufferingAtOnce(num: Int) =
    copy(maxSegmentsToBufferAtOnce = Some(num))

  def inner(ref: Expr.DelayedIdent[J], other: TableExpr.Ident) = {
    copy(
      others = others.appended(
        (ref, "inner", other.key)
      )
    )
  }
  def inner(ref: Expr.DelayedIdent[J]) = {
    copy(
      others = others.appended(
        (ref, "inner", first.name.table)
      )
    )
  }
  def outer(ref: Expr.DelayedIdent[J], other: TableExpr.Ident) = {
    copy(
      others = others.appended(
        (ref, "outer", other.key)
      )
    )
  }
  def outer(ref: Expr.DelayedIdent[J]) = {
    copy(
      others = others.appended(
        (ref, "outer", first.name.table)
      )
    )
  }
  def left(ref: Expr.DelayedIdent[J], other: TableExpr.Ident) = {
    copy(
      others = others.appended(
        (ref, "left", other.key)
      )
    )
  }
  def left(ref: Expr.DelayedIdent[J]) = {
    copy(
      others = others.appended(
        (ref, "left", first.name.table)
      )
    )
  }
  def right(ref: Expr.DelayedIdent[J], other: TableExpr.Ident) = {
    copy(
      others = others.appended(
        (ref, "right", other.key)
      )
    )
  }
  def right(ref: Expr.DelayedIdent[J]) = {
    copy(
      others = others.appended(
        (ref, "right", first.name.table)
      )
    )
  }
  def withPartitionBase(num: Int) = copy(partitionBase = Some(num))
  def withPartitionLimit(num: Int) = copy(partitionLimit = Some(num))
  def done = {
    ra3.tablelang.TableExpr.Join(
      first,
      others,
      partitionBase.getOrElse(128),
      partitionLimit.getOrElse(10_000_000),
      maxSegmentsToBufferAtOnce.getOrElse(10),
      prg
      // prg.getOrElse(ra3.select(ra3.star))
    ).asInstanceOf[ra3.tablelang.TableExpr{type T = self.R}]
  }
}

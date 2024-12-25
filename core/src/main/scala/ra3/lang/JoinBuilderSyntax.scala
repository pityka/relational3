package ra3.lang
import ra3.tablelang.TableExpr

case class JoinBuilder[J](
    private val first: Expr.DelayedIdent[J],
    private val others: Vector[
      (Expr.DelayedIdent[J], String, ra3.tablelang.Key)
    ],
    private val partitionBase: Option[Int],
    private val partitionLimit: Option[Int],
    private val maxItemsToBufferAtOnce: Option[Int]
) {
  def withMaxSegmentsBufferingAtOnce(num: Int) =
    copy(maxItemsToBufferAtOnce = Some(num))

  def inner[N <: Tuple, A <: Tuple](
      ref: Expr.DelayedIdent[J],
      other: TableExpr.Ident[ReturnValueTuple[N, A]]
  ) = {
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
  def outer[N <: Tuple, A <: Tuple](
      ref: Expr.DelayedIdent[J],
      other: TableExpr.Ident[ReturnValueTuple[N, A]]
  ) = {
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
  def left[N <: Tuple, A <: Tuple](
      ref: Expr.DelayedIdent[J],
      other: TableExpr.Ident[ReturnValueTuple[N, A]]
  ) = {
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
  def right[N <: Tuple, A <: Tuple](
      ref: Expr.DelayedIdent[J],
      other: TableExpr.Ident[ReturnValueTuple[N, A]]
  ) = {
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
  def select[N <: Tuple, K <: Tuple](
      prg: ra3.lang.Expr[ReturnValueTuple[N, K]]
  ) = ra3.tablelang.TableExpr.Join(
    first,
    others,
    partitionBase.getOrElse(128),
    partitionLimit.getOrElse(10_000_000),
    maxItemsToBufferAtOnce.getOrElse(1000_000),
    prg
  )
}

private[ra3] object JoinBuilder {
  def apply[J](
      a: Expr.DelayedIdent[J]
  ): JoinBuilder[J] =
    JoinBuilder[J](a, Vector.empty, None, None, None)
}

// /** Builder pattern for joins. Exit the builder with the done method or
//   * elementwise method
//   */
// case class JoinBuilderSyntax[J, K <: Tuple, R <: ReturnValueTuple[K]](
//     private val first: Expr.DelayedIdent[J],
//     private val others: Vector[
//       (Expr.DelayedIdent[J], String, ra3.tablelang.Key)
//     ],
//     val prg: ra3.lang.Expr[R],
//     private val partitionBase: Option[Int],
//     private val partitionLimit: Option[Int],
//     private val maxItemsToBufferAtOnce: Option[Int]
// ) { self =>

//   def done = {
//     ra3.tablelang.TableExpr.Join(
//       first,
//       others,
//       partitionBase.getOrElse(128),
//       partitionLimit.getOrElse(10_000_000),
//       maxItemsToBufferAtOnce.getOrElse(1000_000),
//       prg
//       // prg.getOrElse(ra3.select(ra3.star))
//     )
//   }
// }

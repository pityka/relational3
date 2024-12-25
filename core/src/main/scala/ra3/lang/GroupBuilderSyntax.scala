package ra3.lang

import ra3.tablelang.TableExpr.GroupPartialThenReduce
private[ra3] object GroupBy {
  def apply(
      a: Expr.DelayedIdent[?]
  ) =
    GroupBuilderSyntax(a, Vector.empty, None, None, None)
}

/** Builder pattern for group by clause. Exit the builder with the partial or
  * the all method.
  */
case class GroupBuilderSyntax[T](
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
  def reduceTotal[N <: Tuple, T <: Tuple](
      prg: ra3.lang.Expr[ReturnValueTuple[N, T]]
  ) =
    ra3.tablelang.TableExpr.GroupThenReduce(
      first,
      others,
      prg,
      partitionBase.getOrElse(128),
      partitionLimit.getOrElse(10_000_000),
      maxItemsToBufferAtOnce.getOrElse(1000_000)
    )

  /** Reduces the groups within a partition
    *
    * Rows belonging to other groups are not processed within a group. It is not
    * guaranteed that all rows of the group are processed.
    *
    * Useful for associative reductions followed by further partial or total
    * reduce operations.
    *
    * @param prg
    *   group wise program
    * @return
    */
  def reducePartial[N <: Tuple, T <: Tuple](
      prg: ra3.lang.Expr[ReturnValueTuple[N, T]]
  ): GroupPartialThenReduce[N, T, ReturnValueTuple[N, T]] =
    ra3.tablelang.TableExpr.GroupPartialThenReduce(
      first,
      others,
      prg
    )
  def count[N <: Tuple, T <: Tuple](
      prg: ra3.lang.Expr[ReturnValueTuple[N, T]]
  ) =
    ra3.tablelang.TableExpr.GroupThenCount(
      first,
      others,
      prg,
      partitionBase.getOrElse(128),
      partitionLimit.getOrElse(10_000_000),
      maxItemsToBufferAtOnce.getOrElse(1000_000)
    )

}

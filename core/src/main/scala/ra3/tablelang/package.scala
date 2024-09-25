package ra3

package object tablelang {
    private[ra3] def local(
      assigned: TableExpr
  )(body: Function1[TableExpr.Ident { type T = assigned.T} , TableExpr]) : TableExpr = {
    val n = ra3.tablelang.TagKey(new ra3.tablelang.KeyTag)
    val b = body(TableExpr.Ident(n).asInstanceOf[TableExpr.Ident { type T = assigned.T}])

    TableExpr.Local(n, assigned, b).asInstanceOf[TableExpr]
  }
  
    private[ra3] def localR[R](
      assigned: TableExpr
  )(body: TableExpr.Ident { type T = assigned.T} => TableExpr{type T = R})  = {
    val n = ra3.tablelang.TagKey(new ra3.tablelang.KeyTag)
    val b = body(TableExpr.Ident(n).asInstanceOf[TableExpr.Ident { type T = assigned.T}])

    TableExpr.Local(n, assigned, b).asInstanceOf[TableExpr{type T = R}]
  }
}

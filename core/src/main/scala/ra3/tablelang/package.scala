// package ra3

// package object tablelang {
//     val x = 1
//     private[ra3] def local[A,B](
//       assigned: TableExpr[A]
//   )(body: Function1[TableExpr.Ident[A] , TableExpr[B]]) : TableExpr[B] = {
//     val n = ra3.tablelang.TagKey(new ra3.tablelang.KeyTag)
//     val b = body(TableExpr.Ident(n))

//     TableExpr.Local(n, assigned, b)
//   }

//     private[ra3] def localR[A,R](
//       assigned: TableExpr[A]
//   )(body: TableExpr.Ident[A] => TableExpr[R])  = local(assigned)(body)
// }

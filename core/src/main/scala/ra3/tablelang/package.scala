package ra3

import ra3.lang.{GenericExpr, Expr}

package object tablelang {

  def local[T1](
      assigned: TableExpr
  )(body: TableExpr.Ident => TableExpr): TableExpr = {
    val n = TagKey(new KeyTag)
    val b = body(TableExpr.Ident(n))

    TableExpr.Local(n, assigned, b)
  }
  def let(assigned: TableExpr)(body: TableExpr.Ident => TableExpr): TableExpr =
    local(assigned)(i => body(i))
  def let(assigned: ra3.Table)(body: TableExpr.Ident => TableExpr): TableExpr =
    local(TableExpr.Const(assigned))(body)

  implicit class SyntaxTableExpr(a: TableExpr.Ident) {
    def inner(
        other: TableExpr.Ident,
        colSelf: Int,
        colOther: Int,
        partitionBase: Int,
        partitionLimit: Int
    )(
        prg: ra3.lang.Query
    ) =
      ra3.tablelang.TableExpr.Join(
        a,
        colSelf,
        List((other, colOther, "inner", 0)),
        partitionBase,
        partitionLimit,
        prg
      )
    def groupBy(col: Int, partitionBase: Int, partitionLimit: Int)(
        prg: ra3.lang.Query
    ) =
      ra3.tablelang.TableExpr.GroupThenReduce(
        arg0 = a,
        cols = Right(List(col)),
        partitionBase = partitionBase,
        partitionLimit = partitionLimit,
        groupwise = prg
      )
    def groupBy(col: String, partitionBase: Int, partitionLimit: Int)(
        prg: ra3.lang.Query
    ) =
      ra3.tablelang.TableExpr.GroupThenReduce(
        arg0 = a,
        cols = Left(List(col)),
        partitionBase = partitionBase,
        partitionLimit = partitionLimit,
        groupwise = prg
      )
    def groupBy(cols: Seq[Int], partitionBase: Int, partitionLimit: Int)(
        prg: ra3.lang.Query
    ) =
      ra3.tablelang.TableExpr.GroupThenReduce(
        arg0 = a,
        cols = Right(cols),
        partitionBase = partitionBase,
        partitionLimit = partitionLimit,
        groupwise = prg
      )

    def query(prg: ra3.lang.Query) =
      ra3.tablelang.TableExpr.SimpleQuery(a, prg)

    @scala.annotation.nowarn
    def apply[T0: NotNothing](s: String): ra3.lang.Expr { type T = T0 } =
      Expr.Ident(ra3.lang.Delayed(a.key, Left(s))).as[T0]
    @scala.annotation.nowarn
    def apply[T0: NotNothing](s: Int): ra3.lang.Expr { type T = T0 } =
      Expr.Ident(ra3.lang.Delayed(a.key, Right(s))).as[T0]

    @scala.annotation.nowarn
    private[ra3] def apply[T0: NotNothing, T1: NotNothing](
        n1: String,
        n2: String
    )(
        body: (
            GenericExpr[T0],
            GenericExpr[T1]
        ) => TableExpr
    ) = {
      val d1 = Expr.Ident(ra3.lang.Delayed(a.key, Left(n1))).as[T0]
      val d2 = Expr.Ident(ra3.lang.Delayed(a.key, Left(n2))).as[T1]
      body(d1, d2)
    }
    @scala.annotation.nowarn
    private[ra3] def apply[T0: NotNothing, T1: NotNothing](
        n1: Int,
        n2: Int
    )(
        body: (
            GenericExpr[T0],
            GenericExpr[T1]
        ) => TableExpr
    ) = {
      val d1 = Expr.Ident(ra3.lang.Delayed(a.key, Right(n1))).as[T0]
      val d2 = Expr.Ident(ra3.lang.Delayed(a.key, Right(n2))).as[T1]
      body(d1, d2)
    }

  }

}

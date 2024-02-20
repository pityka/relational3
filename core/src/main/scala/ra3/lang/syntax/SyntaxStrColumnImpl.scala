package ra3.lang.syntax
import ra3.lang._
import ra3.BufferInt
import ra3.lang.StrColumnExpr

trait SyntaxStrColumnImpl {
  protected def arg0: StrColumnExpr
  def parseInt = Expr.makeOp1(ops.Op1.ColumnParseI32OpStr)(arg0)
  def toInt = Expr.makeOp1(ops.Op1.ColumnParseI32OpStr)(arg0)
  def parseDouble = Expr.makeOp1(ops.Op1.ColumnParseF64OpStr)(arg0)
  def toDouble = Expr.makeOp1(ops.Op1.ColumnParseF64OpStr)(arg0)
  def parseLong = Expr.makeOp1(ops.Op1.ColumnParseI64OpStr)(arg0)
  def toLong = Expr.makeOp1(ops.Op1.ColumnParseI64OpStr)(arg0)
  def parseInstant = Expr.makeOp1(ops.Op1.ColumnParseInstOpStr)(arg0)
  def isMissing = Expr.makeOp1(ops.Op1.ColumnIsMissingOpStr)(arg0)

  def !==(arg1: String) = Expr.makeOp2(ops.Op2.ColumnNEqOpStrcStr)(arg0, arg1)
  def ===(arg1: String) = Expr.makeOp2(ops.Op2.ColumnEqOpStrcStr)(arg0, arg1)
  def ===(arg1: StrColumnExpr) = Expr.makeOp2(ops.Op2.ColumnEqOpStrStr)(arg0, arg1)
  def !==(arg1: StrColumnExpr) = Expr.makeOp2(ops.Op2.ColumnNEqOpStrStr)(arg0, arg1)
  def matches(arg1: String) =
    Expr.makeOp2(ops.Op2.ColumnMatchesOpStrcStr)(arg0, arg1)
  def matchAndReplace(pattern: String, replace: String) =
    Expr.makeOp3(ops.Op3.StringMatchAndReplaceOp)(arg0, pattern, replace)
  def containedIn(arg1: Set[String]) =
    Expr.makeOp2(ops.Op2.ColumnContainedInOpStrcStrSet)(arg0, arg1)

  def concatenate(arg1: StrColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnConcatOpStrStr)(arg0, arg1)
  def +(arg1: StrColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnConcatOpStrStr)(arg0, arg1)
  def concatenate(arg1: String) =
    Expr.makeOp2(ops.Op2.ColumnConcatOpStrcStr)(arg0, arg1)
  def +(arg1: String) = Expr.makeOp2(ops.Op2.ColumnConcatOpStrcStr)(arg0, arg1)

  def first = Expr.makeOp3(ops.Op3.BufferFirstGroupsOpSIi)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def count = Expr.makeOp3(ops.Op3.BufferCountInGroupsOpS)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def countDistinct = Expr.makeOp3(ops.Op3.BufferCountDistinctInGroupsOpS)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def hasMissing = Expr.makeOp3(ops.Op3.BufferHasMissingInGroupsOpS)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def substring(start: Int, len: Int) =
    Expr.makeOp3(ops.Op3.BufferSubstringOpS)(
      arg0,
      start,
      len
    )

  def unnamed = ra3.lang.Expr
    .BuiltInOp1(arg0, ops.Op1.MkUnnamedColumnSpecChunk)
    .asInstanceOf[Expr { type T = ColumnSpec }]

  def as(arg1: Expr { type T = String }) = ra3.lang.Expr
    .BuiltInOp2(arg0, arg1, ops.Op2.MkNamedColumnSpecChunk)
    .asInstanceOf[Expr { type T = ColumnSpec }]

  def <=(arg1: StrColumnExpr) = Expr.makeOp2(ops.Op2.ColumnLtEqOpStrStr)(arg0, arg1)
  def <=(arg1: String) = Expr.makeOp2(ops.Op2.ColumnLtEqOpStrcStr)(arg0, arg1)
  def >=(arg1: StrColumnExpr) = Expr.makeOp2(ops.Op2.ColumnGtEqOpStrStr)(arg0, arg1)
  def >=(arg1: String) = Expr.makeOp2(ops.Op2.ColumnGtEqOpStrcStr)(arg0, arg1)

  def <(arg1: StrColumnExpr) = Expr.makeOp2(ops.Op2.ColumnLtOpStrStr)(arg0, arg1)
  def <(arg1: String) = Expr.makeOp2(ops.Op2.ColumnLtOpStrcStr)(arg0, arg1)
  def >(arg1: StrColumnExpr) = Expr.makeOp2(ops.Op2.ColumnGtOpStrStr)(arg0, arg1)
  def >(arg1: String) = Expr.makeOp2(ops.Op2.ColumnGtOpStrcStr)(arg0, arg1)
  

}

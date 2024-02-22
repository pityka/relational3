package ra3.lang.syntax
import ra3.lang._
import ra3.BufferInt



private[ra3] trait SyntaxI32ColumnImpl {

  protected def arg0: I32ColumnExpr
  import scala.language.implicitConversions

  implicit private def conversionI32LitSet(a: Set[Int]): Expr.LitI32Set =
    Expr.LitI32Set(a)
  implicit private def conversionIntLit(a: Int): Expr.LitNum = Expr.LitNum(a)
  implicit private def conversionLongLit(a: Long): Expr.LitI64 = Expr.LitI64(a)
  implicit private def conversionStrLit(a: String): Expr.LitStr = Expr.LitStr(a)
  implicit private def conversionF64Lit(a: Double): Expr.LitF64 = Expr.LitF64(a)

  def abs = Expr.makeOp1(ops.Op1.ColumnAbsOpI)(arg0)
  def isMissing = Expr.makeOp1(ops.Op1.ColumnIsMissingOpI)(arg0)
  def not = Expr.makeOp1(ops.Op1.ColumnNotOpI)(arg0)

  def toDouble = Expr.makeOp1(ops.Op1.ColumnToDoubleOpI)(arg0)

  def ifelseI32(t: I32ColumnExpr, f: I32ColumnExpr) = Expr.makeOp3(ops.Op3.IfElseI32)(arg0, t, f)
  def ifelse(t: I32ColumnExpr, f: Int) = Expr.makeOp3(ops.Op3.IfElseI32C)(arg0, t, f)
  def ifelse(t: Int, f: I32ColumnExpr) = Expr.makeOp3(ops.Op3.IfElseCI32)(arg0, t, f)
  def ifelse(t: Int, f: Int) = Expr.makeOp3(ops.Op3.IfElseI32CC)(arg0, t, f)

  def ifelseI64(t: I64ColumnExpr, f: I64ColumnExpr) = Expr.makeOp3(ops.Op3.IfElseI64)(arg0, t, f)
  def ifelse(t: Long, f: I64ColumnExpr) = Expr.makeOp3(ops.Op3.IfElseCI64)(arg0, t, f)
  def ifelse(t: I64ColumnExpr, f: Long) = Expr.makeOp3(ops.Op3.IfElseI64C)(arg0, t, f)
  def ifelse(t: Long, f: Long) = Expr.makeOp3(ops.Op3.IfElseI64CC)(arg0, t, f)

  def ifelseF64(t: F64ColumnExpr, f: F64ColumnExpr) = Expr.makeOp3(ops.Op3.IfElseF64)(arg0, t, f)
  def ifelse(t: F64ColumnExpr, f: Double) = Expr.makeOp3(ops.Op3.IfElseF64C)(arg0, t, f)
  def ifelse(t: Double, f: F64ColumnExpr) = Expr.makeOp3(ops.Op3.IfElseCF64)(arg0, t, f)
  def ifelse(t: Double, f: Double) = Expr.makeOp3(ops.Op3.IfElseF64CC)(arg0, t, f)

  def ifelseStr(t: StrColumnExpr, f: StrColumnExpr) = Expr.makeOp3(ops.Op3.IfElseStr)(arg0, t, f)
  def ifelse(t: StrColumnExpr, f: String) = Expr.makeOp3(ops.Op3.IfElseStrC)(arg0, t, f)
  def ifelse(t: String, f: StrColumnExpr) = Expr.makeOp3(ops.Op3.IfElseCStr)(arg0, t, f)
  def ifelse(t: String, f: String) = Expr.makeOp3(ops.Op3.IfElseStrCC)(arg0, t, f)

  def ifelseInst(t: InstColumnExpr, f: InstColumnExpr) = Expr.makeOp3(ops.Op3.IfElseInst)(arg0, t, f)
  def ifelseInst(t: InstColumnExpr, f: Long) = Expr.makeOp3(ops.Op3.IfElseInstC)(arg0, t, f)
  def ifelseInst(t: Long, f: InstColumnExpr) = Expr.makeOp3(ops.Op3.IfElseCInst)(arg0, t, f)
  def ifelseInst(t: Long, f: Long) = Expr.makeOp3(ops.Op3.IfElseInstCC)(arg0, t, f)

  def <=(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnLtEqOpII)(arg0, arg1)
  def <=(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnLtEqOpIcI)(arg0, arg1)
  def >=(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnGtEqOpII)(arg0, arg1)
  def >=(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnGtEqOpIcI)(arg0, arg1)

  def <(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnLtOpII)(arg0, arg1)
  def <(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnLtOpIcI)(arg0, arg1)
  def >(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnGtOpII)(arg0, arg1)
  def >(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnGtOpIcI)(arg0, arg1)

  def ===(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnEqOpII)(arg0, arg1)
  def ===(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnEqOpIcI)(arg0, arg1)
  def !==(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnNEqOpII)(arg0, arg1)
  def !==(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnNEqOpIcI)(arg0, arg1)
  def containedIn(arg1: Set[Int]) =
    Expr.makeOp2(ops.Op2.ColumnContainedInOpIcISet)(arg0, arg1)

  def &&(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnAndOpII)(arg0, arg1)
  def ||(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnOrOpII)(arg0, arg1)

  def printf(arg1: String) =
    Expr.makeOp2(ops.Op2.ColumnPrintfOpIcStr)(arg0, Expr.LitStr(arg1))

  def count = Expr.makeOp3(ops.Op3.BufferCountInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def sum = Expr.makeOp3(ops.Op3.BufferSumGroupsOpII)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def min = Expr.makeOp3(ops.Op3.BufferMinGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def max = Expr.makeOp3(ops.Op3.BufferMaxGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def first = Expr.makeOp3(ops.Op3.BufferFirstGroupsOpIIi)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def hasMissing = Expr.makeOp3(ops.Op3.BufferHasMissingInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def all = Expr.makeOp3(ops.Op3.BufferAllInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def any = Expr.makeOp3(ops.Op3.BufferAnyInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def none = Expr.makeOp3(ops.Op3.BufferNoneInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )

  def unnamed = ra3.lang.Expr
    .BuiltInOp1(arg0, ops.Op1.MkUnnamedColumnSpecChunk)
    .asInstanceOf[Expr { type T = ColumnSpec }]

  def as(arg1: Expr { type T = String }) = ra3.lang.Expr
    .BuiltInOp2(arg0, arg1, ops.Op2.MkNamedColumnSpecChunk)
    .asInstanceOf[Expr { type T = ColumnSpec }]

  def as(arg1: String): Expr { type T = ColumnSpec } = as(Expr.LitStr(arg1))

}

package ra3.lang.syntax
import ra3.lang.*
import ra3.BufferInt
import ra3.lang.util.*
import scala.annotation.targetName

private[ra3] trait SyntaxI32ColumnImpl {

  protected def arg0: I32ColumnExpr
  import scala.language.implicitConversions

  implicit private def conversionI32LitSet(a: Set[Int]): Expr[Set[Int]] =
    ra3.LitI32S(a)
  implicit private def conversionIntLit(a: Int): Expr[Int] = ra3.const(a)
  implicit private def conversionLongLit(a: Long): Expr[Long] = ra3.const(a)
  implicit private def conversionStrLit(a: String): Expr[String] = ra3.const(a)
  implicit private def conversionF64Lit(a: Double): Expr[Double] = ra3.const(a)

  def abs = Expr.makeOp1(ops.Op1.ColumnAbsOpI)(arg0)
  def isMissing = Expr.makeOp1(ops.Op1.ColumnIsMissingOpI)(arg0)
  def not = Expr.makeOp1(ops.Op1.ColumnNotOpI)(arg0)

  def toDouble = Expr.makeOp1(ops.Op1.ColumnToDoubleOpI)(arg0)

  def ifelse(t: I32ColumnExpr, f: I32ColumnExpr) =
    Expr.makeOp3(ops.Op3.IfElseI32)(arg0, t, f)
  def ifelse(t: I32ColumnExpr, f: Int) =
    Expr.makeOp3(ops.Op3.IfElseI32C)(arg0, t, f)
  def ifelse(t: Int, f: I32ColumnExpr) =
    Expr.makeOp3(ops.Op3.IfElseCI32)(arg0, t, f)
  def ifelse(t: Int, f: Int) = Expr.makeOp3(ops.Op3.IfElseI32CC)(arg0, t, f)

  @targetName("ifelseI64")
  def ifelse(t: I64ColumnExpr, f: I64ColumnExpr) =
    Expr.makeOp3(ops.Op3.IfElseI64)(arg0, t, f)
  def ifelse(t: Long, f: I64ColumnExpr) =
    Expr.makeOp3(ops.Op3.IfElseCI64)(arg0, t, f)
  def ifelse(t: I64ColumnExpr, f: Long) =
    Expr.makeOp3(ops.Op3.IfElseI64C)(arg0, t, f)
  def ifelse(t: Long, f: Long) = Expr.makeOp3(ops.Op3.IfElseI64CC)(arg0, t, f)

  @targetName("ifelseF64")
  def ifelse(t: F64ColumnExpr, f: F64ColumnExpr) =
    Expr.makeOp3(ops.Op3.IfElseF64)(arg0, t, f)
  def ifelse(t: F64ColumnExpr, f: Double) =
    Expr.makeOp3(ops.Op3.IfElseF64C)(arg0, t, f)
  def ifelse(t: Double, f: F64ColumnExpr) =
    Expr.makeOp3(ops.Op3.IfElseCF64)(arg0, t, f)
  def ifelse(t: Double, f: Double) =
    Expr.makeOp3(ops.Op3.IfElseF64CC)(arg0, t, f)

  @targetName("ifelseStr")
  def ifelse(t: StrColumnExpr, f: StrColumnExpr) =
    Expr.makeOp3(ops.Op3.IfElseStr)(arg0, t, f)
  def ifelse(t: StrColumnExpr, f: String) =
    Expr.makeOp3(ops.Op3.IfElseStrC)(arg0, t, f)
  def ifelse(t: String, f: StrColumnExpr) =
    Expr.makeOp3(ops.Op3.IfElseCStr)(arg0, t, f)
  def ifelse(t: String, f: String) =
    Expr.makeOp3(ops.Op3.IfElseStrCC)(arg0, t, f)

  @targetName("ifelseInst")
  def ifelse(t: InstColumnExpr, f: InstColumnExpr) =
    Expr.makeOp3(ops.Op3.IfElseInst)(arg0, t, f)
  @targetName("ifelseInstL")
  def ifelse(t: InstColumnExpr, f: Long) =
    Expr.makeOp3(ops.Op3.IfElseInstC)(arg0, t, f)
  @targetName("ifelseLInst")
  def ifelse(t: Long, f: InstColumnExpr) =
    Expr.makeOp3(ops.Op3.IfElseCInst)(arg0, t, f)
  def ifelseInst(t: Long, f: Long) =
    Expr.makeOp3(ops.Op3.IfElseInstCC)(arg0, t, f)

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
    Expr.makeOp2(ops.Op2.ColumnPrintfOpIcStr)(arg0, ra3.const(arg1))

  def count = Expr.makeOp3(ops.Op3.BufferCountInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def sum = Expr.makeOp3(ops.Op3.BufferSumGroupsOpII)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def min = Expr.makeOp3(ops.Op3.BufferMinGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def max = Expr.makeOp3(ops.Op3.BufferMaxGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def first = Expr.makeOp3(ops.Op3.BufferFirstGroupsOpIIi)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def hasMissing = Expr.makeOp3(ops.Op3.BufferHasMissingInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def all = Expr.makeOp3(ops.Op3.BufferAllInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def any = Expr.makeOp3(ops.Op3.BufferAnyInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def none = Expr.makeOp3(ops.Op3.BufferNoneInGroupsOpI)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )

  def unnamed = ra3.lang.Expr
    .BuiltInOp1(ops.Op1.MkUnnamedColumnSpecChunkI32)(arg0)

  infix def as(arg1: Expr[String]) = ra3.lang.Expr
    .BuiltInOp2(ops.Op2.MkNamedColumnSpecChunkI32)(arg0, arg1)

  infix def as(arg1: String): Expr[ColumnSpec[ra3.DI32]] = as(ra3.const(arg1))

  @scala.annotation.targetName(":*ColumnSpec")
  infix def :*[T1](v: Expr[ColumnSpec[T1]]) =
    ra3.S.extend(arg0.unnamed).extend(v)
  @scala.annotation.targetName(":*DF64")
  infix def :*(v: Expr[ra3.DF64]) = ra3.S.extend(arg0.unnamed).extend(v.unnamed)
  @scala.annotation.targetName(":*DStr")
  infix def :*(v: Expr[ra3.DStr]) = ra3.S.extend(arg0.unnamed).extend(v.unnamed)
  @scala.annotation.targetName(":*DI32")
  infix def :*(v: Expr[ra3.DI32]) = ra3.S.extend(arg0.unnamed).extend(v.unnamed)
  @scala.annotation.targetName(":*DI64")
  infix def :*(v: Expr[ra3.DI64]) = ra3.S.extend(arg0.unnamed).extend(v.unnamed)
  @scala.annotation.targetName(":*DInst")
  infix def :*(v: Expr[ra3.DInst]) =
    ra3.S.extend(arg0.unnamed).extend(v.unnamed)

}

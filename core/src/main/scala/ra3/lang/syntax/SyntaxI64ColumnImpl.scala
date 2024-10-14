package ra3.lang.syntax
import ra3.lang.*
import ra3.BufferInt
import ra3.lang.util.*

private[ra3] trait SyntaxI64ColumnImpl {
  protected def arg0: I64ColumnExpr
  import scala.language.implicitConversions

  implicit private def conversionI64Lit(a: Long): Expr[Long] = ra3.const(a)
  def isMissing = Expr.makeOp1(ops.Op1.ColumnIsMissingOpL)(arg0)

  def toDouble = Expr.makeOp1(ops.Op1.ColumnToDoubleOpL)(arg0)
  def toInstantEpochMilli =
    Expr.makeOp1(ops.Op1.ColumnToInstantEpochMilliOpL)(arg0)

  def printf(arg1: String) =
    Expr.makeOp2(ops.Op2.ColumnPrintfOpLcStr)(arg0, ra3.const(arg1))

  def count = Expr.makeOp3(ops.Op3.BufferCountInGroupsOpL)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  // def sum = Expr.makeOp3(ops.Op3.BufferSumGroupsOpL)(
  //   arg0,
  //   ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
  //   ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  // )
  // def min = Expr.makeOp3(ops.Op3.BufferMinGroupsOpL)(
  //   arg0,
  //   ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
  //   ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  // )
  // def max = Expr.makeOp3(ops.Op3.BufferMaxGroupsOpL)(
  //   arg0,
  //   ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
  //   ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  // )
  def first = Expr.makeOp3(ops.Op3.BufferFirstGroupsOpL)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def hasMissing = Expr.makeOp3(ops.Op3.BufferHasMissingInGroupsOpL)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )

  def unnamed = ra3.lang.Expr
    .BuiltInOp1(ops.Op1.MkUnnamedColumnSpecChunkI64)(arg0)

  infix def as(arg1: Expr[String]) = ra3.lang.Expr
    .BuiltInOp2(ops.Op2.MkNamedColumnSpecChunkI64)(arg0, arg1)

  infix def as(arg1: String): Expr[ColumnSpec[ra3.DI64]] = as(ra3.const(arg1))

  // def <=(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnLtEqOpII)(arg0, arg1)
  // def <=(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnLtEqOpIcI)(arg0, arg1)
  // def >=(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnGtEqOpII)(arg0, arg1)
  // def >=(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnGtEqOpIcI)(arg0, arg1)

  // def <(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnLtOpII)(arg0, arg1)
  // def <(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnLtOpIcI)(arg0, arg1)
  // def >(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnGtOpII)(arg0, arg1)
  // def >(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnGtOpIcI)(arg0, arg1)

  def ===(arg1: I64ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnEqOpLL)(arg0, arg1)
  def ===(arg1: Long) = Expr.makeOp2(ops.Op2.ColumnEqOpLcL)(arg0, arg1)
  // def !==(arg1: I32ColumnExpr) = Expr.makeOp2(ops.Op2.ColumnNEqOpII)(arg0, arg1)
  // def !==(arg1: Int) = Expr.makeOp2(ops.Op2.ColumnNEqOpIcI)(arg0, arg1)
  // def containedIn(arg1: Set[Int]) =
  //   Expr.makeOp2(ops.Op2.ColumnContainedInOpIcISet)(arg0, arg1)

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

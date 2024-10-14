package ra3.lang.syntax
import ra3.lang.*
import ra3.BufferInt
import ra3.lang.util.*

private[ra3] trait SyntaxStrColumnImpl {
  protected def arg0: StrColumnExpr
  import scala.language.implicitConversions

  implicit private def conversionStrLit(a: String): Expr[String] = ra3.const(a)
  implicit private def conversionStrSetLit(a: Set[String]): Expr[Set[String]] =
    ra3.LitStringS(a)
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
  def ===(arg1: StrColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnEqOpStrStr)(arg0, arg1)
  def !==(arg1: StrColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnNEqOpStrStr)(arg0, arg1)
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
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def count = Expr.makeOp3(ops.Op3.BufferCountInGroupsOpS)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def countDistinct = Expr.makeOp3(ops.Op3.BufferCountDistinctInGroupsOpS)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def hasMissing = Expr.makeOp3(ops.Op3.BufferHasMissingInGroupsOpS)(
    arg0,
    ra3.lang.Expr.Ident[BufferInt](ra3.lang.GroupMap),
    ra3.lang.Expr.Ident[Int](ra3.lang.Numgroups)
  )
  def substring(start: Int, len: Int) =
    Expr.makeOp3(ops.Op3.BufferSubstringOpS)(
      arg0,
      ra3.const(start),
      ra3.const(len)
    )

  def unnamed = ra3.lang.Expr
    .BuiltInOp1(ops.Op1.MkUnnamedColumnSpecChunkStr)(arg0)

  infix def as(arg1: Expr[String]) = ra3.lang.Expr
    .BuiltInOp2(ops.Op2.MkNamedColumnSpecChunkString)(arg0, arg1)

  infix def as(arg1: String): Expr[ColumnSpec[ra3.DStr]] = as(ra3.const(arg1))

  def <=(arg1: StrColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnLtEqOpStrStr)(arg0, arg1)
  def <=(arg1: String) = Expr.makeOp2(ops.Op2.ColumnLtEqOpStrcStr)(arg0, arg1)
  def >=(arg1: StrColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnGtEqOpStrStr)(arg0, arg1)
  def >=(arg1: String) = Expr.makeOp2(ops.Op2.ColumnGtEqOpStrcStr)(arg0, arg1)

  def <(arg1: StrColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnLtOpStrStr)(arg0, arg1)
  def <(arg1: String) = Expr.makeOp2(ops.Op2.ColumnLtOpStrcStr)(arg0, arg1)
  def >(arg1: StrColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnGtOpStrStr)(arg0, arg1)
  def >(arg1: String) = Expr.makeOp2(ops.Op2.ColumnGtOpStrcStr)(arg0, arg1)

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

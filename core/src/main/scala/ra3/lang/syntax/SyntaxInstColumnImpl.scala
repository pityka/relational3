package ra3.lang.syntax
import ra3.lang._
import ra3.BufferInt

private[ra3] trait SyntaxInstColumnImpl {
  protected def arg0: InstColumnExpr
  import scala.language.implicitConversions

  implicit private def conversionStrLit(a: String): Expr.LitStr = Expr.LitStr(a)

  implicit private def conversionIntLit(a: Int): Expr.LitNum = Expr.LitNum(a)
  implicit private def conversionIntLit(a: Long): Expr.LitI64 = Expr.LitI64(a)
  def isMissing = Expr.makeOp1(ops.Op1.ColumnIsMissingOpInst)(arg0)
  def toDoubleEpochMillis = Expr.makeOp1(ops.Op1.ColumnToDoubleOpInst)(arg0)
  def toLongEpochMillis = Expr.makeOp1(ops.Op1.ColumnToLongOpInst)(arg0)
  def toISO = Expr.makeOp1(ops.Op1.ColumnToISOOpInst)(arg0)
  def years = Expr.makeOp1(ops.Op1.ColumnYearsOpInst)(arg0)
  def months = Expr.makeOp1(ops.Op1.ColumnMonthsOpInst)(arg0)
  def days = Expr.makeOp1(ops.Op1.ColumnDaysOpInst)(arg0)
  def hours = Expr.makeOp1(ops.Op1.ColumnHoursOpInst)(arg0)
  def minutes = Expr.makeOp1(ops.Op1.ColumnMinutesOpInst)(arg0)
  def seconds = Expr.makeOp1(ops.Op1.ColumnSecondsOpInst)(arg0)

  def roundToYear = Expr.makeOp1(ops.Op1.ColumnRoundToYearOpInst)(arg0)
  def roundToMonth = Expr.makeOp1(ops.Op1.ColumnRoundToMonthOpInst)(arg0)
  def roundToDay = Expr.makeOp1(ops.Op1.ColumnRoundToDayOpInst)(arg0)
  def roundToHour = Expr.makeOp1(ops.Op1.ColumnRoundToHourOpInst)(arg0)

  def plusSeconds(arg1: Int) =
    Expr.makeOp2(ops.Op2.ColumnPlusSecondsOpInstcInt)(arg0, arg1)
  def minusSeconds(arg1: Int) =
    Expr.makeOp2(ops.Op2.ColumnMinusSecondsOpInstcInt)(arg0, arg1)
  def plusDays(arg1: Int) =
    Expr.makeOp2(ops.Op2.ColumnPlusDaysOpInstcInt)(arg0, arg1)
  def minusDays(arg1: Int) =
    Expr.makeOp2(ops.Op2.ColumnMinusDaysOpInstcInt)(arg0, arg1)

  def <=(arg1: InstColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnLtEqOpInstInst)(arg0, arg1)
  def <=(arg1: String) = Expr.makeOp2(ops.Op2.ColumnLtEqOpInstcStr)(arg0, arg1)
  def <=(arg1: Long) = Expr.makeOp2(ops.Op2.ColumnLtEqOpInstcL)(arg0, arg1)
  def >=(arg1: InstColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnGtEqOpInstInst)(arg0, arg1)
  def >=(arg1: String) = Expr.makeOp2(ops.Op2.ColumnGtEqOpInstcStr)(arg0, arg1)
  def >=(arg1: Long) = Expr.makeOp2(ops.Op2.ColumnGtEqOpInstcL)(arg0, arg1)

  def <(arg1: InstColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnLtOpInstInst)(arg0, arg1)
  def <(arg1: String) = Expr.makeOp2(ops.Op2.ColumnLtOpInstcStr)(arg0, arg1)
  def <(arg1: Long) = Expr.makeOp2(ops.Op2.ColumnLtOpInstcL)(arg0, arg1)
  def >(arg1: InstColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnGtOpInstInst)(arg0, arg1)
  def >(arg1: String) = Expr.makeOp2(ops.Op2.ColumnGtOpInstcStr)(arg0, arg1)
  def >(arg1: Long) = Expr.makeOp2(ops.Op2.ColumnGtOpInstcL)(arg0, arg1)

  def ===(arg1: InstColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnEqOpInstInst)(arg0, arg1)
  def ===(arg1: String) = Expr.makeOp2(ops.Op2.ColumnEqOpInstcStr)(arg0, arg1)
  def ===(arg1: Long) = Expr.makeOp2(ops.Op2.ColumnEqOpInstcL)(arg0, arg1)
  def !==(arg1: InstColumnExpr) =
    Expr.makeOp2(ops.Op2.ColumnNEqOpInstInst)(arg0, arg1)
  def !==(arg1: String) = Expr.makeOp2(ops.Op2.ColumnNEqOpInstcStr)(arg0, arg1)
  def !==(arg1: Long) = Expr.makeOp2(ops.Op2.ColumnNEqOpInstcL)(arg0, arg1)

  def count = Expr.makeOp3(ops.Op3.BufferCountInGroupsOpInst)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )

  def min = Expr.makeOp3(ops.Op3.BufferMinGroupsOpInst)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def max = Expr.makeOp3(ops.Op3.BufferMaxGroupsOpInst)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )

  def first = Expr.makeOp3(ops.Op3.BufferFirstGroupsOpInst)(
    arg0,
    ra3.lang.Expr.Ident(ra3.lang.GroupMap).as[BufferInt],
    ra3.lang.Expr.Ident(ra3.lang.Numgroups).as[Int]
  )
  def hasMissing = Expr.makeOp3(ops.Op3.BufferHasMissingInGroupsOpInst)(
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

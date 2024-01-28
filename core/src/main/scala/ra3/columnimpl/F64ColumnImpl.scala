package ra3.columnimpl
import ra3._
import tasks.TaskSystemComponents
import cats.effect.IO
import ra3.ops.BinaryOpTagDouble._
trait F64ColumnImpl { self: Column.F64Column =>

  def *(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwise(other,dd_multiply)
  def +(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwise(other,dd_add)
  def ===(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dd_eq)
  def !==(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dd_neq)
  def >(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dd_gt)
  def >=(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dd_gteq)
  def <(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dd_lt)
  def <=(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dd_lteq)

  def *(other: Column.I64Column)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwise(other,dl_multiply)
  def +(other: Column.I64Column)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwise(other,dl_add)
  def ===(other: Column.I64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dl_eq)
  def !==(other: Column.I64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dl_neq)
  def >(other: Column.I64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dl_gt)
  def >=(other: Column.I64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dl_gteq)
  def <(other: Column.I64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dl_lt)
  def <=(other: Column.I64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,dl_lteq)

  def *(other: Double)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwiseConstant(other,dd_multiply)
  def +(other: Double)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwiseConstant(other,dd_add)
  def ===(other: Double)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dd_eq)
  def !==(other: Double)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dd_neq)
  def >(other: Double)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dd_gt)
  def >=(other: Double)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dd_gteq)
  def <(other: Double)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dd_lt)
  def <=(other: Double)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dd_lteq)

  def *(other: Long)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwiseConstant(other,dl_multiply)
  def +(other: Long)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwiseConstant(other,dl_add)
  def ===(other: Long)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dl_eq)
  def !==(other: Long)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dl_neq)
  def >(other: Long)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dl_gt)
  def >=(other: Long)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dl_gteq)
  def <(other: Long)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dl_lt)
  def <=(other: Long)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,dl_lteq)

}

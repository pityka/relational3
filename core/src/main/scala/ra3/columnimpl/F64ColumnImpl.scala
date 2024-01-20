package ra3.columnimpl
import ra3._
import tasks.TaskSystemComponents
import cats.effect.IO
trait F64ColumnImpl { self: Column.F64Column =>

  def *(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwise(other,ra3.ops.BinaryOpTag.dd_multiply)
  def +(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwise(other,ra3.ops.BinaryOpTag.dd_add)
  def ===(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ra3.ops.BinaryOpTag.dd_eq)
  def !==(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ra3.ops.BinaryOpTag.dd_neq)
  def >(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ra3.ops.BinaryOpTag.dd_gt)
  def >=(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ra3.ops.BinaryOpTag.dd_gteq)
  def <(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ra3.ops.BinaryOpTag.dd_lt)
  def <=(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ra3.ops.BinaryOpTag.dd_lteq)

}

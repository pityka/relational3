package ra3.columnimpl
import ra3._
import tasks.TaskSystemComponents
import cats.effect.IO
import ra3.ops.BinaryOpTagInt._
trait I32ColumnImpl { self: Column.Int32Column =>
  

  def *(other: Column.Int32Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ii_multiply)
  def +(other: Column.Int32Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ii_add)
  def ===(other: Column.Int32Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ii_eq)
  def !==(other: Column.Int32Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ii_neq)
  def >(other: Column.Int32Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ii_gt)
  def >=(other: Column.Int32Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ii_gteq)
  def <(other: Column.Int32Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ii_lt)
  def <=(other: Column.Int32Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ii_lteq)
  def and(other: Column.Int32Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ii_and)
  def or(other: Column.Int32Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ii_or)

  def *(other: Int)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,ii_multiply)
  def +(other: Int)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,ii_add)
  def ===(other: Int)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,ii_eq)
  def !==(other: Int)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,ii_neq)
  def >(other: Int)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,ii_gt)
  def >=(other: Int)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,ii_gteq)
  def <(other: Int)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,ii_lt)
  def <=(other: Int)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,ii_lteq)
  def and(other: Int)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,ii_and)
  def or(other: Int)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwiseConstant(other,ii_or)

  // IDD
  def *(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwise(other,id_multiply)
  def +(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwise(other,id_add)
  

  def *(other: Double)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwiseConstant(other,id_multiply)
  def +(other: Double)(implicit tsc: TaskSystemComponents) : IO[Column.F64Column] = 
      self.elementwiseConstant(other,id_add)

  // IDI

  def ===(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,id_eq)
  def !==(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,id_neq)
  def >(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,id_gt)
  def >=(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,id_gteq)
  def <(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,id_lt)
  def <=(other: Column.F64Column)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,id_lteq)
 
  
}
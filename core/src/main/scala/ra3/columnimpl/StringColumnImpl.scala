package ra3.columnimpl
import ra3._
import tasks.TaskSystemComponents
import cats.effect.IO
import ra3.ops.BinaryOpTagString._
trait StringColumnImpl { self: Column.StringColumn =>
  

  
  def ===(other: Column.StringColumn)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ss_eq)
  def !==(other: Column.StringColumn)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ss_neq)
  def >(other: Column.StringColumn)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ss_gt)
  def >=(other: Column.StringColumn)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ss_gteq)
  def <(other: Column.StringColumn)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ss_lt)
  def <=(other: Column.StringColumn)(implicit tsc: TaskSystemComponents) : IO[Column.Int32Column] = 
      self.elementwise(other,ss_lteq)
  

 
  
}
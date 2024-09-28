package ra3.lang.ops
import ra3.lang.*
import ra3.*
import tasks.TaskSystemComponents
import cats.effect.*
private[ra3] sealed trait Op4 {
  type A0
  type A1
  type A2
  type A3
  type T
  def op(a: A0, b: A1, c: A2, d: A3)(implicit tsc: TaskSystemComponents): IO[T]
}
private[ra3] sealed trait Op5 {
  type A0
  type A1
  type A2
  type A3
  type A4
  type T
  def op(a: A0, b: A1, c: A2, d: A3, e:A4)(implicit tsc: TaskSystemComponents): IO[T]
}
object OpN {
  class MkReturnValue4[T0,T1,T2,T3] extends Op4 {
    type A0 = ra3.lang.ColumnSpec[T0]
    type A1 = ra3.lang.ColumnSpec[T1]
    type A2 = ra3.lang.ColumnSpec[T2]
    type A3 = ra3.lang.ColumnSpec[T3]
    type T = ra3.lang.ReturnValue4[T0,T1,T2,T3]
    def op(a0: A0, a1: A1, a2: A2, a3: A3)(implicit tsc: TaskSystemComponents) =
      IO.pure(ra3.lang.ReturnValue4(a0, a1, a2,a3, None))
  }
  class MkReturnValue5[T0,T1,T2,T3,T4] extends Op5 {
  type A0 = ra3.lang.ColumnSpec[T0]
    type A1 = ra3.lang.ColumnSpec[T1]
    type A2 = ra3.lang.ColumnSpec[T2]
    type A3 = ra3.lang.ColumnSpec[T3]
    type A4 = ra3.lang.ColumnSpec[T4]
    type T = ra3.lang.ReturnValue5[T0,T1,T2,T3,T4]
    def op(a0: A0, a1: A1, a2: A2, a3: A3, a4:A4)(implicit tsc: TaskSystemComponents) =
      IO.pure(ra3.lang.ReturnValue5(a0, a1, a2,a3,a4, None))
  }
}
// private[ra3] sealed trait OpStar {
//   type A
//   type T
//   def op(a: A*): T
// }

// private[ra3] object OpStar {

//   object MkSelect extends OpStar {
//     type A = ra3.lang.ColumnSpec[Any]
//     type T = ra3.lang.ReturnValue
//     def op(a: A*) = ra3.lang.ReturnValue(ra3.lang.ProjList(List(a*)), None)
//   }
 
// }

package ra3

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.*
import tasks.TaskSystemComponents
import cats.effect.IO
import ra3.lang.*

object Tupler {

  // def test[A <: Tuple:Tuple.IsMappedBy[Option], R](a: A)(
  //   body: Tuple.InverseMap[A,Option] => R
  // ) = body(a.)

  // case class Is[T <: Tuple](value: Tuple.Map[T, CE])
  // case class CE[T](c: ColumnSpecExpr[T])
  // inline def loop[A<:Tuple](tuple: Tuple.Map[A,CE]) : List[CE[?]]= 
  //       inline Is(tuple) match
  //         case _:Is[EmptyTuple] => Nil
  //         case tup : Is[h *: t] =>
  //           tup.value.head :: loop(tup.value.tail)  
  inline def tupleToList[A<:Tuple](tuple: Tuple.Map[A,ColumnSpecExpr]) =       
    tuple.toList
    // loop(tuple).map(_.c.asInstanceOf[ColumnSpecExpr[Any]])
  // def test(t: Tuple) = println(t)

 

  
  // type ColumnSpecExpr[A] = Expr { type T = ColumnSpec[A] }
  // type ReturnExpr[A<:Tuple] = Expr{type T = ReturnValueTuple[A]}
  
  // def select[T<:Tuple](
  //   tup: Tuple.Map[T,ColumnSpecExpr]
  // )  = 
  //   BuiltInOpN(tup,MkReturnValueN)

  // case class Is[T <: Tuple](value: Tuple.Map[T, ColumnSpec])

  // // import compiletime.asMatchable
  // inline def tupleToList[A<:Tuple](tuple: Tuple.Map[A,ColumnSpec]) : List[ColumnSpec[?]]= 
  //   inline Is(tuple) match
  //     case _:Is[EmptyTuple] => Nil
  //     case tup : Is[h *: t] =>
  //       tup.value.head :: tupleToList(tup.value.tail)    
    
  // val pair = (NamedConstantI32(3,"a"),NamedConstantI32(3,"b"))
  // val list = tupleToList[(Int,Int)](pair)

  // inline def list[A<:Tuple](r: ReturnValueTuple[A]) : List[ColumnSpec[?]] = tupleToList[A](r.a0)
  // case class ReturnValueTuple[A <: Tuple](
  //     a0: Tuple.Map[A,ColumnSpec],
  //     filter: Option[DI32]
  // ) extends ReturnValue {
  //   type T = A
  //   def replacePredicate(i: Option[DI32]) = ReturnValueTuple(a0, i)
  // }

  // sealed trait ReturnValue { self =>
  //   type T 
  //   def filter: Option[DI32]
  //   def replacePredicate(i: Option[DI32]): ReturnValue{ type T = self.T}

  // }


  

}

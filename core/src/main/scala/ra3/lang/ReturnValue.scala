package ra3.lang

import ra3.*

object ReturnValue {


  

  def list(r:ReturnValue[?]): List[ColumnSpec[?]] = 
    r match
      case ReturnValueTuple(list, _) =>  list
      case ReturnValueList(a0, filter) => a0.toList
      case ReturnValue1(a0, filter) => List(a0)
      case ReturnValue2(a0, a1, filter) => List(a0,a1)
      case ReturnValue3(a0, a1, a2, filter) => List(a0,a1,a2)
      case ReturnValue4(a0, a1, a2, a3, filter) => List(a0,a1,a2,a3)
      case ReturnValue5(a0, a1, a2, a3, a4, filter) => List(a0,a1,a2,a3,a4)
       
}

sealed trait ReturnValue[+T] { self =>

    def filter: Option[DI32]
    def replacePredicate(i: Option[DI32]): ReturnValue[T]

  }


case class ReturnValueTuple[A <: Tuple](
  list: List[ColumnSpec[Any]],
  filter: Option[DI32],
  ) extends ReturnValue[A] {
    type MM =  Tuple.Map[A,ra3.lang.Expr.DelayedIdent]
    def replacePredicate(i: Option[DI32]) = ReturnValueTuple(list, i)

    def extend[T](v: ColumnSpec[T]) = ReturnValueTuple[Tuple.Append[A,T]](list ++ List(v),filter)
    inline def drop[N<:Int] = {
      val i = scala.compiletime.constValue[N]
      ReturnValueTuple[Tuple.Drop[A,N]](list.drop(i),filter)
    }
   
    def concat[B<:Tuple](b:ReturnValueTuple[B]) = {      
      ReturnValueTuple[Tuple.Concat[A,B]](list ++ b.list,b.filter.orElse(filter))
    }    
  }
object ReturnValueTuple {
  val empty = ReturnValueTuple[EmptyTuple](Nil,None)
}


case class ReturnValueList[A](
    a0: Seq[ColumnSpec[A]],
    filter: Option[DI32]
) extends ReturnValue[Seq[A]] {
  def list = a0.toList
  def replacePredicate(i: Option[DI32]) = ReturnValueList(a0, i)
}

case class ReturnValue1[A](
    a0: ColumnSpec[A],
    filter: Option[DI32]
) extends ReturnValue[A] {
  def replacePredicate(i: Option[DI32]) = ReturnValue1(a0, i)
}
case class ReturnValue2[A, B](
    a0: ColumnSpec[A],
    a1: ColumnSpec[B],
    filter: Option[DI32]
) extends ReturnValue[(A,B)] {
  def replacePredicate(i: Option[DI32]) = ReturnValue2(a0,a1, i)
}
case class ReturnValue3[A0, A1, A2](
    a0: ColumnSpec[A0],
    a1: ColumnSpec[A1],
    a2: ColumnSpec[A2],
    filter: Option[DI32]
) extends ReturnValue[(A0,A1,A2)] {
  def replacePredicate(i: Option[DI32]) = ReturnValue3(a0,a1,a2, i)
}
case class ReturnValue4[A0, A1, A2, A3](
    a0: ColumnSpec[A0],
    a1: ColumnSpec[A1],
    a2: ColumnSpec[A2],
    a3: ColumnSpec[A3],
    filter: Option[DI32]
) extends ReturnValue[(A0,A1,A2,A3)] {
  def replacePredicate(i: Option[DI32]) = ReturnValue4(a0,a1,a2,a3, i)
}
case class ReturnValue5[A0, A1, A2, A3,A4](
    a0: ColumnSpec[A0],
    a1: ColumnSpec[A1],
    a2: ColumnSpec[A2],
    a3: ColumnSpec[A3],
    a4: ColumnSpec[A4],
    filter: Option[DI32]
) extends ReturnValue[(A0,A1,A2,A3,A4)] {
  def replacePredicate(i: Option[DI32]) = ReturnValue5(a0,a1,a2,a3,a4, i)
}

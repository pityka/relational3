package ra3.tablelang
import ra3.lang.Expr
import ra3.lang.ReturnValueTuple
import ra3.NotNothing

case class SchemaCurryT[T0 <: Tuple](
    d: Tuple.Map[T0, ra3.lang.Expr.DelayedIdent]
) {
  inline def apply[R](
      body: (
          Tuple.Map[T0, ra3.lang.Expr.DelayedIdent]
      ) => TableExpr[R]
  ): TableExpr[R] = {

    body(d)
  }
}
object Schema {
  inline def fromIdent[T0 <: Tuple](
      t: TableExpr.Ident[ReturnValueTuple[T0]]
  ) = {
    val size = scala.compiletime.constValue[scala.Tuple.Size[T0]]
    val list = 0 until size map { i =>
      Expr.DelayedIdent[Any](ra3.lang.Delayed(t.key, Right(i)))
    }
    Schema[T0](list)
  }
  inline def fromDelayedIdent[A](e: Expr.DelayedIdent[A]) = {

    Schema[A *: EmptyTuple](List(e.asInstanceOf[Expr.DelayedIdent[Any]]))
  }
}
class Schema[A <: Tuple](
    private val list: Seq[Expr.DelayedIdent[Any]]
) {
  inline def drop[N <: Int] = {
    val i = scala.compiletime.constValue[N]
    Schema[Tuple.Drop[A, N]](list.drop(i))
  }
  inline def take[N <: Int] = {
    val i = scala.compiletime.constValue[N]
    Schema[Tuple.Take[A, N]](list.take(i))
  }
  def concat[B <: Tuple](b: Schema[B]) = {
    Schema[Tuple.Concat[A, B]](list ++ b.list)
  }
  def reverse = {
    Schema[Tuple.Reverse[A]](list.reverse)

  }

  val none: Expr[ReturnValueTuple[EmptyTuple]] = {
    ra3.lang.Expr
      .BuiltInOpAny(
        new ra3.lang.ops.OpAnyUnserializable.MkReturnValueTuple[EmptyTuple]
      )(
        Nil
      )
      .asInstanceOf[Expr[ra3.lang.ReturnValueTuple[EmptyTuple]]]
  }
  val all: Expr[ReturnValueTuple[A]] = {
    // ideally we would fold over the T0 tuple in a macro
    // it did not work so we cast

    //       // type Loop[T<:Tuple,R<:Tuple] = Tuple.Map[T,ra3.lang.DelayedIdent] match {
    //       //   case (Tuple.Head[T] *: Tuple.Map[Tuple.Tail[T],ra3.lang.DelayedIdent]) =>
    //       //     Tuple.Head[T] match {
    //       //       case Expr[ra3.DStr] => Loop[Tuple.Map[Tuple.Tail[T],ra3.lang.DelayedIdent],Tuple.Append[R,Expr[ra3.DStr]]]
    //       //     }
    //       // }
    //       // inline def loop[T<:Tuple,R<:Tuple](t:Tuple.Map[T,ra3.lang.DelayedIdent], acc: Expr[ReturnValueTuple[R]] ) : Loop[T,R] = {
    //       //   t match {
    //       //     case tt: (Tuple.Head[T] *: Tuple.Map[Tuple.Tail[T],ra3.lang.DelayedIdent]) =>
    //       //       val head *: tail = tt
    //       //       import scala.compiletime.asMatchable
    //       //       head.asMatchable match {
    //       //         case head : Expr[ra3.DStr] =>
    //       //           loop(tail,acc.extend(head.unnamed))
    //       //       }
    //       //   }
    val unnameds = list.map(expr =>
      ra3.lang.Expr.BuiltInOp1(ra3.lang.ops.Op1.DynamicUnnamed)(expr)
    )

    ra3.lang.Expr
      .BuiltInOpAny(
        new ra3.lang.ops.OpAnyUnserializable.MkReturnValueTuple[EmptyTuple]
      )(
        unnameds
      )
      .asInstanceOf[Expr[ra3.lang.ReturnValueTuple[A]]]
  }

  private[ra3] inline def columns: SchemaCurryT[A] = {

    val tup = scala.Tuple
      .fromArray(list.toArray)
      .asInstanceOf[Tuple.Map[A, ra3.lang.Expr.DelayedIdent]]

    SchemaCurryT(tup)

  }
}

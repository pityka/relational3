package ra3.tablelang
import ra3.lang.Expr
import ra3.lang.ReturnValueTuple
import ra3.NotNothing
import scala.NamedTuple.NamedTuple
import ra3.lang.ColumnSpec

object Schema {
  inline def fromIdent[N <: Tuple, V <: Tuple](
      t: TableExpr.Ident[NamedTuple[N, V]]
  ) = {
    val size = scala.compiletime.constValue[Tuple.Size[V]]
    val list = 0 until size map { i =>
      Expr.DelayedIdent[ColumnSpec[?, Any]](ra3.lang.Delayed(t.key, Right(i)))
    }
    Schema[N, V](list)
  }

  import scala.compiletime.ops.int.S
  import scala.compiletime.ops.string.+
  type Loop[NN, VV <: Tuple, NR <: Tuple, VR <: Tuple, I <: Int, P <: String] =
    VV match {
      case EmptyTuple => Expr[ReturnValueTuple[NR, VR]]
      case (vh *: vt) =>
        Tuple.Elem[NN, I] match {
          case String =>
            vh match {
              case Expr.DelayedIdent[ra3.F64Var] =>
                Loop[
                  NN,
                  vt,
                  Tuple.Append[NR, P + (Tuple.Elem[NN, I] & String)],
                  Tuple.Append[VR, ra3.F64Var],
                  S[I],
                  P
                ]
              case Expr.DelayedIdent[ra3.StrVar] =>
                Loop[
                  NN,
                  vt,
                  Tuple.Append[NR, P + (Tuple.Elem[NN, I] & String)],
                  Tuple.Append[VR, ra3.StrVar],
                  S[I],
                  P
                ]
              case Expr.DelayedIdent[ra3.I32Var] =>
                Loop[
                  NN,
                  vt,
                  Tuple.Append[NR, P + (Tuple.Elem[NN, I] & String)],
                  Tuple.Append[VR, ra3.I32Var],
                  S[I],
                  P
                ]
              case Expr.DelayedIdent[ra3.I64Var] =>
                Loop[
                  NN,
                  vt,
                  Tuple.Append[NR, P + (Tuple.Elem[NN, I] & String)],
                  Tuple.Append[VR, ra3.I64Var],
                  S[I],
                  P
                ]

              case Expr.DelayedIdent[ra3.InstVar] =>
                Loop[
                  NN,
                  vt,
                  Tuple.Append[NR, P + (Tuple.Elem[NN, I] & String)],
                  Tuple.Append[VR, ra3.InstVar],
                  S[I],
                  P
                ]
            }
        }
    }
  type IsString[X <: String] = X
  inline def loop[
      NN <: Tuple,
      VV <: Tuple,
      NR <: Tuple,
      VR <: Tuple,
      I <: Int,
      P <: String
  ](
      t: VV,
      acc: Expr[ReturnValueTuple[NR, VR]]
  ): Loop[NN, VV, NR, VR, I, P] = {

    inline t match {
      case tt: EmptyTuple => acc
      case tt: (vh *: vt) =>
        inline scala.compiletime.erasedValue[Tuple.Elem[NN, I]] match {
          case a: String =>
            val head *: tail = tt
            inline head match {
              case head: Expr.DelayedIdent[ra3.F64Var] =>
                loop[
                  NN,
                  vt,
                  Tuple.Append[NR, P + (Tuple.Elem[NN, I] & String)],
                  Tuple.Append[VR, ra3.F64Var],
                  S[I],
                  P
                ](
                  tail,
                  acc.extend(
                    ra3.lang.Expr
                      .BuiltInOp1(
                        ra3.lang.ops.Op1.MkNamedColumnSpecChunkF64(
                          scala.compiletime.constValue[P] + scala.compiletime
                            .constValue[a.type]
                        )
                      )(head)
                      .castToName2[P + (Tuple.Elem[NN, I] & String)]
                  )
                )
              case head: Expr.DelayedIdent[ra3.StrVar] =>
                loop[
                  NN,
                  vt,
                  Tuple.Append[NR, P + (Tuple.Elem[NN, I] & String)],
                  Tuple.Append[VR, ra3.StrVar],
                  S[I],
                  P
                ](
                  tail,
                  acc.extend(
                    ra3.lang.Expr
                      .BuiltInOp1(
                        ra3.lang.ops.Op1.MkNamedColumnSpecChunkString(
                          scala.compiletime.constValue[P] + scala.compiletime
                            .constValue[a.type]
                        )
                      )(head)
                      .castToName2[P + (Tuple.Elem[NN, I] & String)]
                  )
                )
              case head: Expr.DelayedIdent[ra3.I32Var] =>
                loop[
                  NN,
                  vt,
                  Tuple.Append[NR, P + (Tuple.Elem[NN, I] & String)],
                  Tuple.Append[VR, ra3.I32Var],
                  S[I],
                  P
                ](
                  tail,
                  acc.extend(
                    ra3.lang.Expr
                      .BuiltInOp1(
                        ra3.lang.ops.Op1.MkNamedColumnSpecChunkI32(
                          scala.compiletime.constValue[P] + scala.compiletime
                            .constValue[a.type]
                        )
                      )(head)
                      .castToName2[P + (Tuple.Elem[NN, I] & String)]
                  )
                )
              case head: Expr.DelayedIdent[ra3.I64Var] =>
                loop[
                  NN,
                  vt,
                  Tuple.Append[NR, P + (Tuple.Elem[NN, I] & String)],
                  Tuple.Append[VR, ra3.I64Var],
                  S[I],
                  P
                ](
                  tail,
                  acc.extend(
                    ra3.lang.Expr
                      .BuiltInOp1(
                        ra3.lang.ops.Op1.MkNamedColumnSpecChunkI64(
                          scala.compiletime.constValue[P] + scala.compiletime
                            .constValue[a.type]
                        )
                      )(head)
                      .castToName2[P + (Tuple.Elem[NN, I] & String)]
                  )
                )

              case head: Expr.DelayedIdent[ra3.InstVar] =>
                loop[
                  NN,
                  vt,
                  Tuple.Append[NR, P + (Tuple.Elem[NN, I] & String)],
                  Tuple.Append[VR, ra3.InstVar],
                  S[I],
                  P
                ](
                  tail,
                  acc.extend(
                    ra3.lang.Expr
                      .BuiltInOp1(
                        ra3.lang.ops.Op1.MkNamedColumnSpecChunkInst(
                          scala.compiletime.constValue[P] + scala.compiletime
                            .constValue[a.type]
                        )
                      )(head)
                      .castToName2[P + (Tuple.Elem[NN, I] & String)]
                  )
                )

            }
        }
    }
  }

  inline def extendAll[
      N <: Tuple,
      V <: Tuple,
      Prefix <: String
  ](
      tuple: NamedTuple[N, V]
  ): Loop[N, V, EmptyTuple.type, EmptyTuple.type, 0, Prefix] = {

    loop[N, V, EmptyTuple, EmptyTuple, 0, Prefix](tuple, ra3.select0)

  }

  def listToReturnValue[N <: Tuple, V <: Tuple](
      list: Seq[Expr.DelayedIdent[ra3.lang.ColumnSpec[?, Any]]]
  ) = {
    ra3.lang.Expr
      .BuiltInOpAny(
        new ra3.lang.ops.OpAnyUnserializable.MkReturnValueTuple[
          EmptyTuple,
          EmptyTuple
        ]
      )(
        list
      )
      .asInstanceOf[Expr[ra3.lang.ReturnValueTuple[N, V]]]
  }

}
class Schema[N <: Tuple, V <: Tuple](
    private val list: Seq[Expr.DelayedIdent[ra3.lang.ColumnSpec[?, Any]]]
) {
  type A = NamedTuple[N, V]

  val all: Expr[ReturnValueTuple[N, V]] = {
    // ideally we would fold over the T0 tuple in a macro
    // it did not work so we cast

    //       // type Loop[T<:Tuple,R<:Tuple] = Tuple.Map[T,ra3.lang.DelayedIdent] match {
    //       //   case (Tuple.Head[T] *: Tuple.Map[Tuple.Tail[T],ra3.lang.DelayedIdent]) =>
    //       //     Tuple.Head[T] match {
    //       //       case Expr[ra3.StrVar] => Loop[Tuple.Map[Tuple.Tail[T],ra3.lang.DelayedIdent],Tuple.Append[R,Expr[ra3.StrVar]]]
    //       //     }
    //       // }
    //       // inline def loop[T<:Tuple,R<:Tuple](t:Tuple.Map[T,ra3.lang.DelayedIdent], acc: Expr[ReturnValueTuple[R]] ) : Loop[T,R] = {
    //       //   t match {
    //       //     case tt: (Tuple.Head[T] *: Tuple.Map[Tuple.Tail[T],ra3.lang.DelayedIdent]) =>
    //       //       val head *: tail = tt
    //       //       import scala.compiletime.asMatchable
    //       //       head.asMatchable match {
    //       //         case head : Expr[ra3.StrVar] =>
    //       //           loop(tail,acc.extend(head.unnamed))
    //       //       }
    //       //   }
    // val unnameds = list.map(expr =>
    //   ra3.lang.Expr.BuiltInOp1(ra3.lang.ops.Op1.DynamicUnnamed)(expr)
    // )

    ra3.lang.Expr
      .BuiltInOpAny(
        new ra3.lang.ops.OpAnyUnserializable.MkReturnValueTuple[
          EmptyTuple,
          EmptyTuple
        ]
      )(
        list
      )
      .asInstanceOf[Expr[ra3.lang.ReturnValueTuple[N, V]]]
  }

  private[ra3] inline def columns[R](
      inline body: (
          scala.NamedTuple.Map[A, ra3.lang.Expr.DelayedIdent]
      ) => TableExpr[R]
  ): TableExpr[R] =
    body(columnsDirect)

  private[ra3] inline def columnsDirect
      : scala.NamedTuple.Map[A, ra3.lang.Expr.DelayedIdent] =
    scala.Tuple
      .fromArray(list.toArray)
      .asInstanceOf[scala.NamedTuple.Map[A, ra3.lang.Expr.DelayedIdent]]

}

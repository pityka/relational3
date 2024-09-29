// package ra3

// import java.util.concurrent.TimeUnit
// import java.util.concurrent.TimeUnit.*
// import tasks.TaskSystemComponents
// import cats.effect.IO
// import ra3.lang.*

// object Tupler {

  

//   // type Primitive[+T<:Primitives] <: Primitives = T match {
//   //   case DI32 => DI32
//   //   case DI64 => DI64
//   //   case DF64 => DF64
//   //   case DStr => DStr
//   //   case DInst => DInst
//   // }
          
//   // case class Is[T <: Tuple](value: T)
//   type Extract[T<:Tuple] <: Tuple = T match {
//     case EmptyTuple => EmptyTuple 
//     case Expr[ColumnSpec[h]] *: t => h *: Extract[t]
//   }
//   type Checked[T<:Tuple] <: Tuple = T match {
//     case EmptyTuple => EmptyTuple 
//     case h *: t => 
//       h match {
//         case Expr[ColumnSpec[DStr]] => ColumnSpecExpr[DStr] *: Checked[t]                          
//         case Expr[ColumnSpec[DF64]] => ColumnSpecExpr[DF64] *: Checked[t] 
//         case Expr[DI64] => ColumnSpecExpr[DI64] *: Checked[t]
//         case Expr[DF64] => ColumnSpecExpr[DF64] *: Checked[t]
//         case Expr[String] => ColumnSpecExpr[String] *: Checked[t]
//         case Expr[DStr] => ColumnSpecExpr[DStr] *: Checked[t]
//       }
//   }
//   import scala.compiletime.asMatchable
//   inline def check[T<:Tuple](tuple: T) : Checked[T] = 
//      {inline tuple match {
//       case _:EmptyTuple => EmptyTuple
//       case tt : (h *: t) =>
//           val head *: tail = tt
//           inline head.asMatchable match {
//             case ce: ColumnSpecExpr[DStr] => ce *: check(tail)
//             case ce: ColumnSpecExpr[DF64] => ce *: check(tail)
//             case ce:I64ColumnExpr => ce.unnamed *: check(tail)
//             case ce:F64ColumnExpr => ce.unnamed *: check(tail)
//             case ce: Expr[String] => ce.unnamed *: check(tail)
//             case ce: Expr[DStr] => ce.unnamed *: check(tail)
//           }
          
//      }}

//     val t  = check((Expr.LitStr(""),null.asInstanceOf[ColumnSpecExpr[DStr]], null.asInstanceOf[I64ColumnExpr]))

     


  
     

  

// }

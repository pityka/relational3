package ra3.lang

 sealed trait Value[T] {
    def v: T
  }
  object Value {
    case class Const[T](v: T) extends Value[T]

    // case class Func(call: Seq[Value] => Value) extends Value
  }
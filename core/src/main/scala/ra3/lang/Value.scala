package ra3.lang

private[ra3] sealed trait Value[+T] {
  def v: T
}
private[ra3] object Value {
  case class Const[T](v: T) extends Value[T]

  // case class Func(call: Seq[Value] => Value) extends Value
}

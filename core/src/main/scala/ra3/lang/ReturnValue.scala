package ra3.lang

import ra3._

private[ra3] trait ColumnSpec[+T]
private[ra3] sealed trait ColumnSpecWithValue[T] extends ColumnSpec[T] {
  def value: T
}
private[ra3] sealed trait NamedColumnSpec[T] extends ColumnSpecWithValue[T] {
  def name: String
}
private[ra3] sealed trait UnnamedColumnSpec[T] extends ColumnSpecWithValue[T] {
  def withName(s: String): NamedColumnSpec[T]
}
private[ra3] case class NamedColumnChunk(
    value: Either[Buffer, Seq[Segment]],
    name: String
) extends NamedColumnSpec[Either[Buffer, Seq[Segment]]]
private[ra3] case class NamedConstantI32(value: Int, name: String)
    extends NamedColumnSpec[Int]
private[ra3] case class NamedConstantI64(value: Long, name: String)
    extends NamedColumnSpec[Long]
private[ra3] case class NamedConstantString(value: String, name: String)
    extends NamedColumnSpec[String]
private[ra3] case class NamedConstantF64(value: Double, name: String)
    extends NamedColumnSpec[Double]
private[ra3] case class UnnamedColumnChunk(value: Either[Buffer, Seq[Segment]])
    extends UnnamedColumnSpec[Either[Buffer, Seq[Segment]]] {
  def withName(s: String): NamedColumnChunk = NamedColumnChunk(value, s)
}
private[ra3] case class UnnamedConstantI32(value: Int)
    extends UnnamedColumnSpec[Int] {
  def withName(s: String): NamedColumnSpec[Int] = NamedConstantI32(value, s)
}
private[ra3] case class UnnamedConstantI64(value: Long)
    extends UnnamedColumnSpec[Long] {
  def withName(s: String): NamedColumnSpec[Long] = NamedConstantI64(value, s)
}
private[ra3] case class UnnamedConstantString(value: String)
    extends UnnamedColumnSpec[String] {
  def withName(s: String): NamedColumnSpec[String] =
    NamedConstantString(value, s)
}
private[ra3] case class UnnamedConstantF64(value: Double)
    extends UnnamedColumnSpec[Double] {
  def withName(s: String): NamedColumnSpec[Double] = NamedConstantF64(value, s)
}
private[ra3] object StarColumnSpec extends ColumnSpec[Any]


sealed trait ReturnValue {
    type T <: ReturnValue
  def list: List[ColumnSpec[_]]
  def filter: Option[DI32]
  def replacePredicate(i: Option[DI32]): T
  
}


private[ra3] case class ReturnValueList[A](
    a0: Seq[ColumnSpec[A]],
    filter: Option[DI32]
) extends ReturnValue {
  def list = a0.toList
  type T = ReturnValueList[A]
  def replacePredicate(i: Option[DI32]) = ReturnValueList(a0, i)
}

private[ra3] case class ReturnValue1[A](
    a0: ColumnSpec[A],
    filter: Option[DI32]
) extends ReturnValue {
  def list = List(a0)
  type T = ReturnValue1[A]
  def replacePredicate(i: Option[DI32]) = ReturnValue1(a0, i)
}
private[ra3] case class ReturnValue2[A, B](
    a0: ColumnSpec[A],
    a1: ColumnSpec[B],
    filter: Option[DI32]
) extends ReturnValue {
  def list = List(a0,a1)
  type T = ReturnValue2[A,B]
  def replacePredicate(i: Option[DI32]) = ReturnValue2(a0,a1, i)
}
private[ra3] case class ReturnValue3[A0, A1, A2](
    a0: ColumnSpec[A0],
    a1: ColumnSpec[A1],
    a2: ColumnSpec[A2],
    filter: Option[DI32]
) extends ReturnValue {
  def list = List(a0,a1,a2)
  type T = ReturnValue3[A0,A1,A2]
  def replacePredicate(i: Option[DI32]) = ReturnValue3(a0,a1,a2, i)
}

package test
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

object Lang {

  object Op {
    sealed trait Op2 {
      type A
      type B
      type C
      def op(a: A, b: B): C
    }

    sealed trait Op2III extends Op2 {
      type A = Int
      type B = Int
      type C = Int
    }

    case object AddOp extends Op2III {
      def op(a: Int, b: Int): Int = a + b
    }
  }

  sealed trait Expr {
    type T
    private[Lang] def evalWith(env: Map[String, Value[_]]): Value[T]
  }
  private[Lang] object Expr {
    implicit val codec: JsonValueCodec[Expr] =
      JsonCodecMaker.make(CodecMakerConfig.withAllowRecursiveTypes(true))

    case class LitStr(s: String) extends Expr {
      type T = String
      def evalWith(env: Map[String, Value[_]]): Value[T] = Value.Const(s)
    }
    case class LitNum(s: Int) extends Expr {
      type T = Int
      def evalWith(env: Map[String, Value[_]]): Value[T] = Value.Const(s)
    }
    case class Ident(name: String) extends Expr {

      def evalWith(env: Map[String, Value[_]]): Value[T] =
        env(name).asInstanceOf[Value[T]]
      def as[T1] = asInstanceOf[Expr { type T = T1 }]
    }

    case class BuiltInOp2(arg0: Expr, arg1: Expr, op: Op.Op2) extends Expr {
      type T = op.C

      def evalWith(env: Map[String, Value[_]]): Value[T] =
        Value.Const(
          op.op(
            arg0.asInstanceOf[Expr { type T = op.A }].evalWith(env).v,
            arg1.asInstanceOf[Expr { type T = op.B }].evalWith(env).v
          )
        )

    }

    private[Lang] def makeOp2(
        arg0: Expr,
        arg1: Expr,
        op: Op.Op2
    ): Expr { type T = op.C } =
      BuiltInOp2(arg0, arg1, op).asInstanceOf[Expr { type T = op.C }]

    def Add(arg0: Expr { type T = Int }, arg1: Expr { type T = Int }) =
      makeOp2(arg0, arg1, Op.AddOp)

    case class Local(name: String, assigned: Expr, body: Expr) extends Expr {
      self =>
      type T = body.T
      def evalWith(env: Map[String, Value[_]]): Value[T] = {
        val assignedValue = assigned.evalWith(env)
        body.evalWith(env + (name -> assignedValue))
      }

    }

    // case class Func(argNames: Seq[String], body: Expr) extends Expr
    // case class Call(expr: Expr, args: Seq[Expr]) extends Expr

  }

  sealed trait Value[T] {
    def v: T
  }
  object Value {
    case class Const[T](v: T) extends Value[T]

    // case class Func(call: Seq[Value] => Value) extends Value
  }

  type IntExpr = Expr { type T = Int }
  implicit class SyntaxInt(arg0: IntExpr) {
    def +(arg1: IntExpr) = Expr.makeOp2(arg0, arg1, Op.AddOp)
  }
  implicit class SyntaxIntLit(a: Int) {
    def lift = Expr.LitNum(a)
  }
  import scala.language.implicitConversions
  implicit def conversion(a: Int): Expr.LitNum = Expr.LitNum(a)

  def local(assigned: Expr)(body: Expr { type T = assigned.T } => Expr) = {
    val n = scala.util.Random.alphanumeric.take(6).mkString
    val id = Expr.Ident(n)
    val b = body(id.as[assigned.T])
    Expr.Local(n, assigned, b)
  }
  def global[T0](n: String)(body: Expr { type T = T0 } => Expr) =
    local(Expr.Ident(n).as[T0])(body)

  def evaluate(expr: Expr): Value[expr.T] = expr.evalWith(Map.empty)
  def evaluate(expr: Expr, map: Map[String, Value[_]]): Value[expr.T] =
    expr.evalWith(map)
  // // case Expr.Call(expr, args) =>
  // //   val Value.Func(call) = evaluate(expr, scope)
  // //   val evaluatedArgs = args.map(evaluate(_, scope))
  // //   call(evaluatedArgs)
  // // case Expr.Func(argNames, body) =>
  // //   Value.Func(args => evaluate(body, scope ++ argNames.zip(args)))
  // }

}

sealed trait Buffer { self =>
  type Elem
  type BufferType >: this.type <: Buffer
  type SegmentType <: Segment {
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
    type Elem = self.Elem
  }
  type ColumnType <: Column {
    type Elem = self.Elem
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
  }
  def refine = this.asInstanceOf[BufferType]
  def toSeq: Seq[Elem]
  def toSegment: SegmentType
  def both(other: BufferType): BufferType

}
sealed trait Segment { self =>
  type SegmentType >: this.type <: Segment
  type Elem
  type BufferType <: Buffer {
    type Elem = self.Elem
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
  }
  type ColumnType <: Column {
    type Elem = self.Elem
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
  }

  def buffer: BufferType
}
sealed trait Column { self =>

  type ColumnType >: this.type <: Column
  type Elem
  type BufferType <: Buffer {
    type Elem = self.Elem
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
  }
  type SegmentType <: Segment {
    type Elem = self.Elem
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
  }
  type ColumnTagType <: ColumnTag {
    type ColumnType = self.ColumnType
  }

  def segments: Vector[SegmentType]

  def ++(other: ColumnType): ColumnType

}
sealed trait ColumnTag { self =>
  type ColumnTagType >: this.type <: ColumnTag
  type ColumnType <: Column {
    type ColumnTagType = self.ColumnTagType
  }
}

case class Int32Column(segments: Vector[SegmentInt]) extends Column {

  override def ++(other: Int32Column): Int32Column = Int32Column(
    segments ++ other.segments
  )

  type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Int32Column

}

object Column {
  implicit val codec: JsonValueCodec[Column] = JsonCodecMaker.make

}

case class SegmentInt(values: Array[Int]) extends Segment {
  type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Int32Column

  override def buffer = BufferInt(values)

}
case class BufferInt(values: Array[Int]) extends Buffer { self =>

  type Elem = Int
  type BufferType = BufferInt
  type SegmentType = SegmentInt
  type ColumnType = Int32Column

  def both(other: BufferType): BufferType = ???

  override def toSeq: Seq[Int] = values.toSeq

  override def toSegment: SegmentInt = ???

  def filterInEquality[B <: Buffer { type BufferType = B }](
      comparison: B,
      cutoff: B
  ): Unit = ???

}

object Segment {
  implicit val codec: JsonValueCodec[Segment] = JsonCodecMaker.make

}

object Test extends App {

  import Lang._
  val e: Lang.Expr =
    Lang.global[Int]("hole")(hole =>
      Lang.local(1.lift)(greeting => hole + greeting + 2)
    )

  println(e)
  println(writeToString(e))
  val e2 = readFromString[Expr](writeToString(e))
  println(e2)
  println(evaluate(e, Map("hole" -> Value.Const(1))))
  println(evaluate(e2, Map("hole" -> Value.Const(1))))
  // val e = Local("greeting", LitNum(1), Ident("greeting"))

  println(evaluate(e))

  ???
  val b1 = BufferInt(Array(1))
  val b2 = BufferInt(Array(2))
  val segment1 = b1.toSegment
  val segment2 = b2.toSegment
  // val tpe = Int32
  val c: Column = Int32Column(Vector(segment1, segment2))
  // val d = c.z
  val t: Vector[SegmentInt] = Vector(segment1.buffer.toSegment, segment1)
  implicitly[JsonValueCodec[Segment]]
  implicitly[JsonValueCodec[Column]]
  val t0 = c.segments
  val t1 = c.segments.map(_.buffer)
  t1.reduce((a, b) => {
    val r = a.both(b)
    r
  })

  segment1.buffer
    .filterInEquality[segment1.BufferType](segment1.buffer, segment2.buffer)

  def m1[S <: Segment](
      input: S
  ): S = ???
  def m2(
      input: Segment
  ): Segment = ???
}

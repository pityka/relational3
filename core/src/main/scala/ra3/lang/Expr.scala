package ra3.lang
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import tasks.TaskSystemComponents

class KeyTag
sealed trait Key
sealed trait SingletonKey extends Key
case object GroupMap extends SingletonKey
case object Numgroups extends SingletonKey
case class IntKey(s: Int) extends Key
case class TagKey(s: KeyTag) extends Key
case class ColumnKey(tableUniqueId: String, columnIdx: Int) extends Key



sealed trait Expr { self =>
  type T

  private[lang] def evalWith(env: Map[Key, Value[_]])(implicit tsc:TaskSystemComponents): IO[Value[T]]
  def hash = {
    val bytes = writeToArray(this.replaceTags())
    com.google.common.hash.Hashing.murmur3_128().hashBytes(bytes).asLong()
  }
  // utilities for analysis of the tree
  private[lang] def tags: Set[KeyTag]
  private[ra3] def columnKeys: Set[ColumnKey]
  private[lang] def replace(map: Map[KeyTag, Int]): Expr
  private[ra3] def replaceTags() =
    replace(this.tags.toSeq.zipWithIndex.toMap)

}
private[lang] object Expr {

  implicit val codec: JsonValueCodec[Expr] =
    JsonCodecMaker.make(CodecMakerConfig.withAllowRecursiveTypes(true))

  case object Star extends Expr {
    type T = ColumnSpec
    def evalWith(env: Map[Key, Value[_]])(implicit tsc:TaskSystemComponents): IO[Value[T]] = IO.pure(Value.Const(ra3.lang.Star))
    def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty
    def replace(i: Map[KeyTag, Int]): Expr = this
  }
  case class LitStr(s: String) extends Expr {
    type T = String
    def evalWith(env: Map[Key, Value[_]])(implicit tsc:TaskSystemComponents): IO[Value[T]] = IO.pure(Value.Const(s))
    def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty
    def replace(i: Map[KeyTag, Int]): Expr = this
  }
  case class LitNum(s: Int) extends Expr {
    type T = Int
    def evalWith(env: Map[Key, Value[_]])(implicit tsc:TaskSystemComponents): IO[Value[T]] = IO.pure(Value.Const(s))
    def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty
    def replace(i: Map[KeyTag, Int]): Expr = this
  }
  case class Ident(name: Key) extends Expr {

    def evalWith(env: Map[Key, Value[_]])(implicit tsc:TaskSystemComponents): IO[Value[T]] =
      IO.pure(env(name).asInstanceOf[Value[T]])
    def as[T1] = asInstanceOf[Expr { type T = T1 }]

    def tags: Set[KeyTag] = name match {
      case TagKey(s) => Set(s)
      case _         => Set.empty
    }
    val columnKeys: Set[ColumnKey] = name match {
      case a: ColumnKey => Set(a)
      case _            => Set.empty
    }
    override def replace(i: Map[KeyTag, Int]) = name match {
      case TagKey(s) => Ident(IntKey(i(s)))
      case _         => this
    }
  }

  case class BuiltInOp1(arg0: Expr, op: ops.Op1) extends Expr {
    type T = op.T

    val tags: Set[KeyTag] = arg0.tags
    val columnKeys: Set[ColumnKey] = arg0.columnKeys
    def replace(i: Map[KeyTag, Int]): Expr = BuiltInOp1(arg0.replace(i), op)

    def evalWith(env: Map[Key, Value[_]])(implicit tsc:TaskSystemComponents): IO[Value[T]] =
      arg0.asInstanceOf[Expr { type T = op.A0 }].evalWith(env).flatMap(v => 
        op.op(v.v).map(Value.Const(_))
      
      )

  }
  case class BuiltInOp2(arg0: Expr, arg1: Expr, op: ops.Op2) extends Expr {
    type T = op.T

    val tags: Set[KeyTag] = arg0.tags ++ arg1.tags
    val columnKeys: Set[ColumnKey] = arg0.columnKeys ++ arg1.columnKeys
    def replace(i: Map[KeyTag, Int]): Expr =
      BuiltInOp2(arg0.replace(i), arg1.replace(i), op)

    def evalWith(env: Map[Key, Value[_]])(implicit tsc:TaskSystemComponents): IO[Value[T]] =
      for {
        a0 <- arg0.asInstanceOf[Expr { type T = op.A0 }].evalWith(env)
        a1 <- arg1.asInstanceOf[Expr { type T = op.A1 }].evalWith(env)
        r <- op.op(a0.v,a1.v)
      } yield Value.Const(r)
      

  }
  case class BuiltInOp3(arg0: Expr, arg1: Expr, arg2: Expr, op: ops.Op3)
      extends Expr {
    type T = op.T

    val tags: Set[KeyTag] = arg0.tags ++ arg1.tags ++ arg2.tags
    val columnKeys: Set[ColumnKey] =
      arg0.columnKeys ++ arg1.columnKeys ++ arg2.columnKeys
    def replace(i: Map[KeyTag, Int]): Expr =
      BuiltInOp3(arg0.replace(i), arg1.replace(i), arg2.replace(i), op)

    def evalWith(env: Map[Key, Value[_]])(implicit tsc:TaskSystemComponents): IO[Value[T]] =
      for {
        a0 <- arg0.asInstanceOf[Expr { type T = op.A0 }].evalWith(env)
        a1 <- arg1.asInstanceOf[Expr { type T = op.A1 }].evalWith(env)
        a2 <- arg2.asInstanceOf[Expr { type T = op.A2 }].evalWith(env)
        r <- op.op(a0.v,a1.v,a2.v)
      } yield Value.Const(r)
      

  }
  case class BuiltInOpStar(args: Seq[Expr], op: ops.OpStar)
      extends Expr {
    type T = op.T

    val tags: Set[KeyTag] = args.flatMap(_.tags).toSet
    val columnKeys: Set[ColumnKey] =
      args.flatMap(_.columnKeys).toSet
    def replace(i: Map[KeyTag, Int]): Expr =
      BuiltInOpStar(args.map(_.replace(i)), op)

    def evalWith(env: Map[Key, Value[_]])(implicit tsc:TaskSystemComponents): IO[Value[T]] =
          IO.parSequenceN(32)(args.map(_.asInstanceOf[Expr { type T = op.A }].evalWith(env))).map( args =>
            Value.Const(op.op(args.map(_.v):_*)))
        

  }

  def makeOp3(op: ops.Op3)(
      arg0: Expr { type T = op.A0 },
      arg1: Expr { type T = op.A1 },
      arg2: Expr { type T = op.A2 }
  ): Expr { type T = op.T } =
    BuiltInOp3(arg0, arg1, arg2, op).asInstanceOf[Expr { type T = op.T }]
  def makeOp2(op: ops.Op2)(
      arg0: Expr { type T = op.A0 },
      arg1: Expr { type T = op.A1 }
  ): Expr { type T = op.T } =
    BuiltInOp2(arg0, arg1, op).asInstanceOf[Expr { type T = op.T }]

  def makeOp1(op: ops.Op1)(
      arg0: Expr { type T = op.A0 }
  ): Expr { type T = op.T } =
    BuiltInOp1(arg0, op).asInstanceOf[Expr { type T = op.T }]
  def makeOpStar(op: ops.OpStar)(
      args: Expr { type T = op.A }*
  ): Expr { type T = op.T } =
    BuiltInOpStar(args, op).asInstanceOf[Expr { type T = op.T }]

  case class Local(name: Key, assigned: Expr, body: Expr) extends Expr {
    self =>
    type T = body.T

    val tags: Set[KeyTag] = name match {
      case TagKey(s) => Set(s) ++ assigned.tags ++ body.tags
      case _         => assigned.tags ++ body.tags
    }

    val columnKeys: Set[ColumnKey] = assigned.columnKeys ++ body.columnKeys

    def replace(i: Map[KeyTag, Int]): Expr = {
      val replacedName = name match {
        case TagKey(t) => IntKey(i(t))
        case n         => n
      }
      Local(replacedName, assigned.replace(i), body.replace(i))
    }
    def evalWith(env: Map[Key, Value[_]])(implicit tsc:TaskSystemComponents): IO[Value[T]] = {
      assigned.evalWith(env).flatMap{ assignedValue =>
      body.evalWith(env + (name -> assignedValue))
      }
    }

  }

}

package ra3.lang
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.tablelang.DelayedTableSchema
import ra3.tablelang
import ra3.lang.StrExpr
import ra3._

private[ra3] class KeyTag
private[ra3] sealed trait Key
private[ra3] sealed trait SingletonKey extends Key
private[ra3] case object GroupMap extends SingletonKey
private[ra3] case object Numgroups extends SingletonKey
private[ra3] case class IntKey(s: Int) extends Key
private[ra3] case class TagKey(s: KeyTag) extends Key
private[ra3] case class ColumnKey(tableUniqueId: String, columnIdx: Int)
    extends Key
private[ra3] case class Delayed(
    private[ra3] table: ra3.tablelang.Key,
    private[ra3] selection: Either[String, Int]
) extends Key

private[ra3] sealed trait Expr { self =>
  private[ra3] type T

  private[lang] def evalWith(env: Map[Key, Value[_]])(implicit
      tsc: TaskSystemComponents
  ): IO[Value[T]]
  private[ra3] def hash = {
    val bytes = writeToArray(this.replaceTags(Map.empty))
    com.google.common.hash.Hashing.murmur3_128().hashBytes(bytes).asLong()
  }
  // utilities for analysis of the tree
  private[lang] def tags: Set[KeyTag]
  private[ra3] def columnKeys: Set[ColumnKey]
  private[ra3] def referredTables: Set[ra3.tablelang.TableExpr.Ident]
  private[lang] def replace(
      map: Map[KeyTag, Int],
      map2: Map[ra3.tablelang.KeyTag, Int]
  ): Expr
  private[ra3] def replaceDelayed(map: DelayedTableSchema): Expr
  private[ra3] def replaceTags(map2: Map[ra3.tablelang.KeyTag, Int]) =
    replace(this.tags.toSeq.zipWithIndex.toMap, map2)

  def in[T1](body: Expr {
    type T = self.T
  } => Expr {
    type T = T1
  }): Expr { type T = T1 } = {
    val n = ra3.lang.TagKey(new ra3.lang.KeyTag)
    val b = body(Expr.Ident(n).as[self.T])

    Expr.Local(n, this, b).asInstanceOf[Expr { type T = T1 }]
  }

}
object Expr {

  implicit class SyntaxExpr[T0](a: Expr { type T = T0 })
      extends ra3.lang.syntax.SyntaxExprImpl[T0] {
    // type T00 = T0
    protected val arg0 = a

  }
  implicit class SyntaxListExpr[T0](a: Expr { type T = List[T0] }) {
    // type T00 = T0
    protected val arg0 = a
    def ::(arg1: Expr { type T <: T0 }) =
      ra3.lang.Expr
        .BuiltInOp2(arg1, arg0, ops.Op2.Cons)
        .asInstanceOf[Expr { type T = List[T0] }]

  }
  implicit class SyntaxReturnExpr[T0](a: ReturnExpr) {
    // type T00 = T0
    protected val arg0 = a
    def where(arg1: I32ColumnExpr): ReturnExpr =
      Expr.makeOp2(ops.Op2.MkReturnWhere)(arg0, arg1)

  }
  implicit class SyntaxColumnF64(a: F64ColumnExpr)
      extends syntax.SyntaxF64ColumnImpl {
    protected def arg0 = a

  }
  implicit class SyntaxColumnInt(a: I32ColumnExpr)
      extends syntax.SyntaxI32ColumnImpl {
    protected def arg0 = a

  }
  implicit class SyntaxColumnLong(a: I64ColumnExpr)
      extends syntax.SyntaxI64ColumnImpl {
    protected def arg0 = a

  }
  implicit class SyntaxColumnStr(a: StrColumnExpr)
      extends syntax.SyntaxStrColumnImpl {
    protected def arg0 = a

  }
  implicit class SyntaxColumnInst(a: InstColumnExpr)
      extends syntax.SyntaxInstColumnImpl {
    protected def arg0 = a

  }
  implicit class SyntaxColumn(a: ColumnExpr) extends syntax.SyntaxColumnImpl {
    protected def arg0 = a

  }
  implicit class SyntaxInt(a: Int) extends syntax.SyntaxIntImpl {
    protected def arg0 = Expr.LitNum(a)

  }
  implicit class SyntaxIntExpr(a: IntExpr) extends syntax.SyntaxIntImpl {
    protected def arg0 = a

  }

  implicit class SyntaxStrLit(a: String) extends syntax.SyntaxStringImpl {
    protected def arg0 = Expr.LitStr(a)
    def lift = arg0
  }
  implicit class SyntaxStrLitExpr(a: StrExpr) extends syntax.SyntaxStringImpl {
    protected def arg0 = a
    def lift = arg0
  }

  implicit class SyntaxLongLit(a: Long) extends syntax.SyntaxLongImpl {
    protected def arg0 = Expr.LitI64(a)
    def lift = arg0
  }
  implicit class SyntaxStrLongExpr(a: LongExpr) extends syntax.SyntaxLongImpl {
    protected def arg0 = a
    def lift = arg0
  }

  implicit class SyntaxDoubleLit(a: Double) extends syntax.SyntaxDoubleImpl {
    protected def arg0 = Expr.LitF64(a)
    def lift = arg0
  }
  implicit class SyntaxStrDoubleExpr(a: DoubleExpr)
      extends syntax.SyntaxDoubleImpl {
    protected def arg0 = a
    def lift = arg0
  }

  import scala.language.implicitConversions
  implicit def conversionDI32(
      a: Expr { type T = DI32 }
  ): Expr { type T = ColumnSpec } = a.unnamed

  implicit def conversionDF64(
      a: Expr { type T = DF64 }
  ): Expr { type T = ColumnSpec } = a.unnamed
  implicit def conversionDI64(
      a: Expr { type T = DI64 }
  ): Expr { type T = ColumnSpec } = a.unnamed
  implicit def conversionDInst(
      a: Expr { type T = DInst }
  ): Expr { type T = ColumnSpec } = a.unnamed

  implicit def conversionStr(
      a: Expr { type T = DStr }
  ): Expr { type T = ColumnSpec } = a.unnamed

  implicit val customCodecOfDouble: JsonValueCodec[Double] =
    ra3.Utils.customDoubleCodec

  implicit val codec: JsonValueCodec[Expr] =
    JsonCodecMaker.make(CodecMakerConfig.withAllowRecursiveTypes(true))

  private[ra3] case object Star extends Expr {

    private[ra3] def referredTables = Set.empty
    type T = ColumnSpec
    def evalWith(env: Map[Key, Value[_]])(implicit
        tsc: TaskSystemComponents
    ): IO[Value[T]] = IO.pure(Value.Const(ra3.lang.Star))
    def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      this
    def replaceDelayed(map: DelayedTableSchema) = this
  }
  private[ra3] case class LitStrSet(s: Set[String]) extends Expr {

    private[ra3] def referredTables = Set.empty
    type T = Set[String]
    def evalWith(env: Map[Key, Value[_]])(implicit
        tsc: TaskSystemComponents
    ): IO[Value[T]] = IO.pure(Value.Const(s))
    def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      this
    def replaceDelayed(map: DelayedTableSchema) = this
  }
  private[ra3] case class LitStr(s: String) extends Expr {

    private[ra3] def referredTables = Set.empty
    type T = String
    def evalWith(env: Map[Key, Value[_]])(implicit
        tsc: TaskSystemComponents
    ): IO[Value[T]] = IO.pure(Value.Const(s))
    def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      this
    def replaceDelayed(map: DelayedTableSchema) = this
  }
  private[ra3] case class LitNum(s: Int) extends Expr {

    private[ra3] def referredTables = Set.empty
    type T = Int
    def evalWith(env: Map[Key, Value[_]])(implicit
        tsc: TaskSystemComponents
    ): IO[Value[T]] = IO.pure(Value.Const(s))
    def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      this
    def replaceDelayed(map: DelayedTableSchema) = this
  }
  private[ra3] case class LitF64(s: Double) extends Expr {

    private[ra3] def referredTables = Set.empty
    type T = Double
    def evalWith(env: Map[Key, Value[_]])(implicit
        tsc: TaskSystemComponents
    ): IO[Value[T]] = IO.pure(Value.Const(s))
    def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      this
    def replaceDelayed(map: DelayedTableSchema) = this
  }
  private[ra3] case class LitF64Set(s: Set[Double]) extends Expr {

    private[ra3] def referredTables = Set.empty
    type T = Set[Double]
    def evalWith(env: Map[Key, Value[_]])(implicit
        tsc: TaskSystemComponents
    ): IO[Value[T]] = IO.pure(Value.Const(s))
    def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      this
    def replaceDelayed(map: DelayedTableSchema) = this
  }
  private[ra3] case class LitI32Set(s: Set[Int]) extends Expr {

    private[ra3] def referredTables = Set.empty
    type T = Set[Int]
    def evalWith(env: Map[Key, Value[_]])(implicit
        tsc: TaskSystemComponents
    ): IO[Value[T]] = IO.pure(Value.Const(s))
    def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      this
    def replaceDelayed(map: DelayedTableSchema) = this
  }

  private[ra3] case class LitI64(s: Long) extends Expr {

    private[ra3] def referredTables = Set.empty
    type T = Long
    def evalWith(env: Map[Key, Value[_]])(implicit
        tsc: TaskSystemComponents
    ): IO[Value[T]] = IO.pure(Value.Const(s))
    def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      this
    def replaceDelayed(map: DelayedTableSchema) = this
  }

  case class DelayedIdent private[ra3] (private[ra3] val name: Delayed)
      extends Expr {

    private[ra3] def referredTables = Set(tableIdent)

    /** Join this column with an other column */
    def join = ra3.lang.Join(this)

    /** group the table by this column */
    def groupBy = ra3.lang.GroupBy(this)

    private[ra3] def tableIdent = ra3.tablelang.TableExpr.Ident(name.table)
    def table = tableIdent

    private[ra3] def evalWith(env: Map[Key, Value[_]])(implicit
        tsc: TaskSystemComponents
    ): IO[Value[T]] =
      IO.pure(env(name).asInstanceOf[Value[T]])
    private[ra3] def cast[T1] = asInstanceOf[DelayedIdent { type T = T1 }]

    private[ra3] def tags: Set[KeyTag] = Set.empty
    private[ra3] val columnKeys: Set[ColumnKey] = Set.empty

    private[ra3] override def replace(
        i: Map[KeyTag, Int],
        i2: Map[ra3.tablelang.KeyTag, Int]
    ) = name.table match {
      case tablelang.TagKey(s) =>
        DelayedIdent(Delayed(ra3.tablelang.IntKey(i2(s)), name.selection))
      case _ => this
    }

    private[ra3] override def replaceDelayed(i: DelayedTableSchema) =
      name match {
        case x: Delayed => Ident(i.replace(x))
        case _          => this
      }
  }
  private[ra3] case class Ident(name: Key) extends Expr {

    private[ra3] def referredTables = name match {

      case Delayed(table, _) => Set(ra3.tablelang.TableExpr.Ident(table))
      case _                 => Set.empty
    }

    def evalWith(env: Map[Key, Value[_]])(implicit
        tsc: TaskSystemComponents
    ): IO[Value[T]] =
      IO.pure(env(name).asInstanceOf[Value[T]])
    def as[T1] = asInstanceOf[Expr { type T = T1 }]

    def tags: Set[KeyTag] = name match {
      case TagKey(s) => Set(s)

      case _ => Set.empty
    }
    val columnKeys: Set[ColumnKey] = name match {
      case a: ColumnKey => Set(a)
      case _            => Set.empty
    }
    override def replace(
        i: Map[KeyTag, Int],
        i2: Map[ra3.tablelang.KeyTag, Int]
    ) = name match {
      case TagKey(s) => Ident(IntKey(i(s)))
      case Delayed(ra3.tablelang.TagKey(t), s) =>
        Ident(Delayed(ra3.tablelang.IntKey(i2(t)), s))
      case _ => this
    }

    override def replaceDelayed(i: DelayedTableSchema) = name match {
      case x: Delayed => Ident(i.replace(x))
      case _          => this
    }
  }

  private[ra3] case class BuiltInOp1(arg0: Expr, op: ops.Op1) extends Expr {
    type T = op.T

    private[ra3] def referredTables = arg0.referredTables
    val tags: Set[KeyTag] = arg0.tags
    val columnKeys: Set[ColumnKey] = arg0.columnKeys
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      BuiltInOp1(arg0.replace(i, i2), op)
    def replaceDelayed(i: ra3.tablelang.DelayedTableSchema): Expr =
      BuiltInOp1(arg0.replaceDelayed(i), op)

    def evalWith(
        env: Map[Key, Value[_]]
    )(implicit tsc: TaskSystemComponents): IO[Value[T]] =
      arg0
        .asInstanceOf[Expr { type T = op.A0 }]
        .evalWith(env)
        .flatMap(v => op.op(v.v).map(Value.Const(_)))

  }
  private[ra3] case class BuiltInOp2(arg0: Expr, arg1: Expr, op: ops.Op2)
      extends Expr {
    type T = op.T

    private[ra3] def referredTables = arg0.referredTables ++ arg1.referredTables

    val tags: Set[KeyTag] = arg0.tags ++ arg1.tags
    val columnKeys: Set[ColumnKey] = arg0.columnKeys ++ arg1.columnKeys
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      BuiltInOp2(arg0.replace(i, i2), arg1.replace(i, i2), op)

    def replaceDelayed(i: DelayedTableSchema): Expr =
      BuiltInOp2(arg0.replaceDelayed(i), arg1.replaceDelayed(i), op)

    def evalWith(
        env: Map[Key, Value[_]]
    )(implicit tsc: TaskSystemComponents): IO[Value[T]] =
      for {
        a0 <- arg0.asInstanceOf[Expr { type T = op.A0 }].evalWith(env)
        a1 <- arg1.asInstanceOf[Expr { type T = op.A1 }].evalWith(env)
        r <- op.op(a0.v, a1.v)
      } yield Value.Const(r)

  }
  private[ra3] case class BuiltInOp3(
      arg0: Expr,
      arg1: Expr,
      arg2: Expr,
      op: ops.Op3
  ) extends Expr {
    type T = op.T

    private[ra3] def referredTables =
      arg0.referredTables ++ arg1.referredTables ++ arg2.referredTables

    val tags: Set[KeyTag] = arg0.tags ++ arg1.tags ++ arg2.tags
    val columnKeys: Set[ColumnKey] =
      arg0.columnKeys ++ arg1.columnKeys ++ arg2.columnKeys
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      BuiltInOp3(
        arg0.replace(i, i2),
        arg1.replace(i, i2),
        arg2.replace(i, i2),
        op
      )

    def replaceDelayed(i: DelayedTableSchema): Expr = BuiltInOp3(
      arg0.replaceDelayed(i),
      arg1.replaceDelayed(i),
      arg2.replaceDelayed(i),
      op
    )
    def evalWith(
        env: Map[Key, Value[_]]
    )(implicit tsc: TaskSystemComponents): IO[Value[T]] =
      for {
        a0 <- arg0.asInstanceOf[Expr { type T = op.A0 }].evalWith(env)
        a1 <- arg1.asInstanceOf[Expr { type T = op.A1 }].evalWith(env)
        a2 <- arg2.asInstanceOf[Expr { type T = op.A2 }].evalWith(env)
        r <- op.op(a0.v, a1.v, a2.v)
      } yield Value.Const(r)

  }
  private[ra3] case class BuiltInOpStar(args: Seq[Expr], op: ops.OpStar)
      extends Expr {
    type T = op.T

    private[ra3] def referredTables = args.flatMap(_.referredTables).toSet

    val tags: Set[KeyTag] = args.flatMap(_.tags).toSet
    val columnKeys: Set[ColumnKey] =
      args.flatMap(_.columnKeys).toSet
    def replace(i: Map[KeyTag, Int], i2: Map[ra3.tablelang.KeyTag, Int]): Expr =
      BuiltInOpStar(args.map(_.replace(i, i2)), op)

    def replaceDelayed(i: DelayedTableSchema): Expr =
      BuiltInOpStar(args.map(_.replaceDelayed(i)), op)

    def evalWith(
        env: Map[Key, Value[_]]
    )(implicit tsc: TaskSystemComponents): IO[Value[T]] =
      IO.parSequenceN(32)(
        args.map(_.asInstanceOf[Expr { type T = op.A }].evalWith(env))
      ).map(args => Value.Const(op.op(args.map(_.v): _*)))

  }

  private[ra3] def makeOp3(op: ops.Op3)(
      arg0: Expr { type T = op.A0 },
      arg1: Expr { type T = op.A1 },
      arg2: Expr { type T = op.A2 }
  ): Expr { type T = op.T } =
    BuiltInOp3(arg0, arg1, arg2, op).asInstanceOf[Expr { type T = op.T }]
  private[ra3] def makeOp2(op: ops.Op2)(
      arg0: Expr { type T = op.A0 },
      arg1: Expr { type T = op.A1 }
  ): Expr { type T = op.T } =
    BuiltInOp2(arg0, arg1, op).asInstanceOf[Expr { type T = op.T }]

  private[ra3] def makeOp1(op: ops.Op1)(
      arg0: Expr { type T = op.A0 }
  ): Expr { type T = op.T } =
    BuiltInOp1(arg0, op).asInstanceOf[Expr { type T = op.T }]
  private[ra3] def makeOpStar(op: ops.OpStar)(
      args: Expr { type T = op.A }*
  ): Expr { type T = op.T } =
    BuiltInOpStar(args, op).asInstanceOf[Expr { type T = op.T }]

  private[ra3] case class Local(name: Key, assigned: Expr, body: Expr)
      extends Expr {
    self =>
    type T = body.T

    private[ra3] def referredTables =
      assigned.referredTables ++ body.referredTables

    val tags: Set[KeyTag] = name match {
      case TagKey(s) => Set(s) ++ assigned.tags ++ body.tags
      case _         => assigned.tags ++ body.tags
    }

    val columnKeys: Set[ColumnKey] = assigned.columnKeys ++ body.columnKeys

    def replace(
        i: Map[KeyTag, Int],
        i2: Map[ra3.tablelang.KeyTag, Int]
    ): Expr = {
      val replacedName = name match {
        case TagKey(t) => IntKey(i(t))
        case Delayed(ra3.tablelang.TagKey(t), s) =>
          Delayed(ra3.tablelang.IntKey(i2(t)), s)
        case n => n
      }
      Local(replacedName, assigned.replace(i, i2), body.replace(i, i2))
    }
    def replaceDelayed(i: DelayedTableSchema): Expr = {
      val replacedName = name match {
        case d: Delayed => i.replace(d)
        case n          => n
      }
      Local(replacedName, assigned.replaceDelayed(i), body.replaceDelayed(i))
    }
    def evalWith(
        env: Map[Key, Value[_]]
    )(implicit tsc: TaskSystemComponents): IO[Value[T]] = {
      assigned.evalWith(env).flatMap { assignedValue =>
        body.evalWith(env + (name -> assignedValue))
      }
    }

  }

}

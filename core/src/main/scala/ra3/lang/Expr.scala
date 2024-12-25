package ra3.lang
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.tablelang.DelayedTableSchema
import ra3.tablelang
import ra3.lang.util.*
import ra3.*

class KeyTag
sealed trait Key
sealed trait SingletonKey extends Key
case object GroupMap extends SingletonKey
case object Numgroups extends SingletonKey
case class IntKey(s: Int) extends Key
case class TagKey(s: KeyTag) extends Key
case class ColumnKey(tableUniqueId: String, columnIdx: Int) extends Key
case class Delayed(
    table: ra3.tablelang.Key,
    selection: Either[String, Int]
) extends Key

sealed trait Expr[+T] { self =>

  protected def toRuntime: runtime.Expr

  // utilities for analysis of the tree
  protected def tags: Set[KeyTag]
  def referredTables: Set[ra3.tablelang.TableExpr.Ident[Any]]
  protected def replace(
      map: Map[KeyTag, Int],
      map2: Map[ra3.tablelang.KeyTag, Int]
  ): Expr[T]
  protected def replaceDelayed(map: DelayedTableSchema): Expr[T]
  private def replaceTags() =
    replace(this.tags.toSeq.zipWithIndex.toMap, Map.empty)

  def in[T1](body: Expr[T] => Expr[T1]): Expr[T1] = {
    val n = ra3.lang.TagKey(new ra3.lang.KeyTag)
    val b = body(Expr.Ident[T](n))

    Expr.Local(n, this, b)
  }

}
object Expr {

  extension [N <: String, T](arg0: Expr[ColumnSpec[N, T]]) {
    @scala.annotation.targetName(":*ColumnSpec")
    infix def :*[N1 <: String, T1](v: Expr[ColumnSpec[N1, T1]]) =
      ra3.S.extend(arg0).extend(v)

  }
  def toRuntime[T](e: Expr[T], dt: DelayedTableSchema) = {
    e.replaceDelayed(
      dt
    ).replaceTags()
      .toRuntime
  }

  // implicit class SyntaxReturnExpr[T0 <: Tuple](a: Expr[ReturnValueTuple[T0]])
  //     extends ra3.lang.syntax.SyntaxReturnExprImpl[T0] {
  //   protected val arg0 = a

  // }
  implicit class SyntaxReturnExprNamed[N <: Tuple, T0 <: Tuple](
      a: Expr[ReturnValueTuple[N, T0]]
  ) extends ra3.lang.syntax.SyntaxReturnExprImplNamed[N, T0] {
    protected val arg0 = a

  }
  implicit class SyntaxExpr[T0](a: Expr[T0])
      extends ra3.lang.syntax.SyntaxExprImpl[T0] {
    // type T00 = T0
    protected val arg0 = a

  }
  implicit class SyntaxExprStringColumnSpecString[T0](
      a: Expr[ColumnSpec[String, T0]]
  ) {
    // this cast is needed because I can't thread type variable across serialization
    def castToName[N <: String] = a.asInstanceOf[Expr[ColumnSpec[N, T0]]]
    def castToName2[N] = a.asInstanceOf[Expr[ColumnSpec[N, T0]]]

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

  implicit class SyntaxInt(a: Int) extends syntax.SyntaxIntImpl {
    protected def arg0 = ra3.const(a)

  }
  implicit class SyntaxIntExpr(a: IntExpr) extends syntax.SyntaxIntImpl {
    protected def arg0 = a

  }

  implicit class SyntaxStrLit(a: String) extends syntax.SyntaxStringImpl {
    protected def arg0 = ra3.const(a)
    def lift = arg0
  }
  implicit class SyntaxStrLitExpr(a: StrExpr) extends syntax.SyntaxStringImpl {
    protected def arg0 = a
    def lift = arg0
  }

  implicit class SyntaxLongLit(a: Long) extends syntax.SyntaxLongImpl {
    protected def arg0 = ra3.const(a)
    def lift = arg0
  }
  implicit class SyntaxStrLongExpr(a: LongExpr) extends syntax.SyntaxLongImpl {
    protected def arg0 = a
    def lift = arg0
  }

  implicit class SyntaxDoubleLit(a: Double) extends syntax.SyntaxDoubleImpl {
    protected def arg0 = ra3.const(a)
    def lift = arg0
  }
  implicit class SyntaxStrDoubleExpr(a: DoubleExpr)
      extends syntax.SyntaxDoubleImpl {
    protected def arg0 = a
    def lift = arg0
  }

  implicit val customCodecOfDouble: JsonValueCodec[Double] =
    ra3.Utils.customDoubleCodec

  final case class DelayedIdent[TT](
      val name: Delayed
  ) extends Expr[TT] { self =>

    def toRuntime: ra3.lang.runtime.Expr = runtime.Expr.DelayedIdent(name)

    def referredTables = Set(tableIdent)

    /** Join this column with an other column */
    def inner(other: DelayedIdent[TT]) =
      ra3.lang.JoinBuilder[TT](this).inner(other)

    /** Join this column with an other column */
    def outer(other: DelayedIdent[TT]) =
      ra3.lang.JoinBuilder[TT](this).outer(other)

    /** Join this column with an other column */
    def left(other: DelayedIdent[TT]) =
      ra3.lang.JoinBuilder[TT](this).left(other)

    /** Join this column with an other column */
    def right(other: DelayedIdent[TT]) =
      ra3.lang.JoinBuilder[TT](this).right(other)

    /** group the table by this column */
    def groupBy =
      ra3.lang.GroupBy(this)

    /** partition the table by this column */
    def partitionBy =
      ra3.lang.PrepartitionBy(this)

    def topK(
        ascending: Boolean,
        k: Int,
        cdfCoverage: Double,
        cdfNumberOfSamplesPerSegment: Int
    ) = ra3.tablelang.TableExpr.TopK(
      this,
      ascending,
      k,
      cdfCoverage,
      cdfNumberOfSamplesPerSegment
    )

    def tableIdent: ra3.tablelang.TableExpr.Ident[TT] =
      ra3.tablelang.TableExpr
        .Ident(name.table)

    def table = tableIdent

    protected def tags: Set[KeyTag] = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty

    protected override def replace(
        i: Map[KeyTag, Int],
        i2: Map[ra3.tablelang.KeyTag, Int]
    ): DelayedIdent[TT] = name.table match {
      case tablelang.TagKey(s) =>
        DelayedIdent[TT](Delayed(ra3.tablelang.IntKey(i2(s)), name.selection))
      case _ => this
    }

    protected override def replaceDelayed(i: DelayedTableSchema) =
      Ident(i.replace(name))
  }
  final case class Ident[T](name: Key) extends Expr[T] {

    def toRuntime: ra3.lang.runtime.Expr = runtime.Expr.Ident(name)

    def referredTables = name match {

      case Delayed(table, _) => Set(ra3.tablelang.TableExpr.Ident(table))
      case _                 => Set.empty
    }

    def tags: Set[KeyTag] = name match {
      case TagKey(s) => Set(s)

      case _ => Set.empty
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

  final case class BuiltInOp0[R](op: ops.Op0 { type T = R }) extends Expr[R] {

    def toRuntime: ra3.lang.runtime.Expr = runtime.Expr.BuiltInOp0(op)

    def referredTables = Set.empty
    val tags: Set[KeyTag] = Set.empty
    def replace(
        i: Map[KeyTag, Int],
        i2: Map[ra3.tablelang.KeyTag, Int]
    ): Expr[op.T] = this
    def replaceDelayed(i: ra3.tablelang.DelayedTableSchema): Expr[op.T] = this

  }
  final case class BuiltInOp1[R](val op: ops.Op1 { type T = R })(
      val arg0: Expr[op.A0]
  ) extends Expr[R] {

    def toRuntime: ra3.lang.runtime.Expr =
      runtime.Expr.BuiltInOp1(op, arg0.toRuntime)

    def referredTables = arg0.referredTables
    val tags: Set[KeyTag] = arg0.tags
    def replace(
        i: Map[KeyTag, Int],
        i2: Map[ra3.tablelang.KeyTag, Int]
    ): Expr[op.T] =
      BuiltInOp1(op)(arg0.replace(i, i2))
    def replaceDelayed(i: ra3.tablelang.DelayedTableSchema): Expr[op.T] =
      BuiltInOp1(op)(arg0.replaceDelayed(i))

  }
  final case class BuiltInOp2[R](op: ops.Op2 { type T = R })(
      val arg0: Expr[op.A0],
      val arg1: Expr[op.A1]
  ) extends Expr[R] {

    def toRuntime: ra3.lang.runtime.Expr =
      runtime.Expr.BuiltInOp2(op, arg0.toRuntime, arg1.toRuntime)

    def referredTables = arg0.referredTables ++ arg1.referredTables

    val tags: Set[KeyTag] = arg0.tags ++ arg1.tags
    def replace(
        i: Map[KeyTag, Int],
        i2: Map[ra3.tablelang.KeyTag, Int]
    ): Expr[op.T] =
      BuiltInOp2(op)(arg0.replace(i, i2), arg1.replace(i, i2))

    def replaceDelayed(i: DelayedTableSchema): Expr[op.T] =
      BuiltInOp2(op)(arg0.replaceDelayed(i), arg1.replaceDelayed(i))

  }
  final case class BuiltInOp2US[R](op: ops.Op2Unserializable {
    type T = R
  })(val arg0: Expr[op.A0], val arg1: Expr[op.A1])
      extends Expr[R] {

    def toRuntime: ra3.lang.runtime.Expr =
      runtime.Expr.BuiltInOp2(op.erase, arg0.toRuntime, arg1.toRuntime)

    def referredTables = arg0.referredTables ++ arg1.referredTables

    val tags: Set[KeyTag] = arg0.tags ++ arg1.tags
    def replace(
        i: Map[KeyTag, Int],
        i2: Map[ra3.tablelang.KeyTag, Int]
    ): Expr[op.T] =
      BuiltInOp2US(op)(arg0.replace(i, i2), arg1.replace(i, i2))

    def replaceDelayed(i: DelayedTableSchema): Expr[op.T] =
      BuiltInOp2US(op)(arg0.replaceDelayed(i), arg1.replaceDelayed(i))

  }
  final case class BuiltInOp3[R](op: ops.Op3 { type T = R })(
      val arg0: Expr[op.A0],
      val arg1: Expr[op.A1],
      val arg2: Expr[op.A2]
  ) extends Expr[R] {

    def toRuntime: ra3.lang.runtime.Expr = runtime.Expr.BuiltInOp3(
      op,
      arg0.toRuntime,
      arg1.toRuntime,
      arg2.toRuntime
    )

    def referredTables =
      arg0.referredTables ++ arg1.referredTables ++ arg2.referredTables

    val tags: Set[KeyTag] = arg0.tags ++ arg1.tags ++ arg2.tags
    def replace(
        i: Map[KeyTag, Int],
        i2: Map[ra3.tablelang.KeyTag, Int]
    ): Expr[op.T] =
      BuiltInOp3(op)(
        arg0.replace(i, i2),
        arg1.replace(i, i2),
        arg2.replace(i, i2)
      )

    def replaceDelayed(i: DelayedTableSchema): Expr[op.T] = BuiltInOp3(op)(
      arg0.replaceDelayed(i),
      arg1.replaceDelayed(i),
      arg2.replaceDelayed(i)
    )

  }

  final case class BuiltInOpAny[R](op: ops.OpAnyUnserializable {
    type T = R
  })(val args: Seq[Expr[op.A]])
      extends Expr[R] {

    def toRuntime: ra3.lang.runtime.Expr =
      runtime.Expr.BuiltInOpAny(op.erased, args.map(_.toRuntime))
    def referredTables = args.flatMap(_.referredTables).toSet

    val tags: Set[KeyTag] = args.flatMap(_.tags).toSet
    def replace(
        i: Map[KeyTag, Int],
        i2: Map[ra3.tablelang.KeyTag, Int]
    ): Expr[R] =
      BuiltInOpAny(op)(args.map(_.replace(i, i2)))

    def replaceDelayed(i: DelayedTableSchema): Expr[R] =
      BuiltInOpAny(op)(args.map(_.replaceDelayed(i)))

  }

  def makeOp3(op: ops.Op3)(
      arg0: Expr[op.A0],
      arg1: Expr[op.A1],
      arg2: Expr[op.A2]
  ): Expr[op.T] =
    BuiltInOp3(op)(arg0, arg1, arg2)
  def makeOp2(op: ops.Op2)(
      arg0: Expr[op.A0],
      arg1: Expr[op.A1]
  ): Expr[op.T] =
    BuiltInOp2(op)(arg0, arg1)
  def makeOp2(op: ops.Op2Unserializable)(
      arg0: Expr[op.A0],
      arg1: Expr[op.A1]
  ): Expr[op.T] =
    BuiltInOp2US(op)(arg0, arg1)

  def makeOp1(op: ops.Op1)(
      arg0: Expr[op.A0]
  ): Expr[op.T] =
    BuiltInOp1(op)(arg0)
  def makeOp0(op: ops.Op0): Expr[op.T] =
    BuiltInOp0(op)

  final case class Local[R](
      name: Key,
      assigned: Expr[?],
      body: Expr[R]
  ) extends Expr[R] {
    self =>

    def toRuntime: ra3.lang.runtime.Expr =
      runtime.Expr.Local(name, assigned.toRuntime, body.toRuntime)

    def referredTables =
      assigned.referredTables ++ body.referredTables

    val tags: Set[KeyTag] = name match {
      case TagKey(s) => Set(s) ++ assigned.tags ++ body.tags
      case _         => assigned.tags ++ body.tags
    }

    def replace(
        i: Map[KeyTag, Int],
        i2: Map[ra3.tablelang.KeyTag, Int]
    ): Expr[R] = {
      val replacedName = name match {
        case TagKey(t) => IntKey(i(t))
        case Delayed(ra3.tablelang.TagKey(t), s) =>
          Delayed(ra3.tablelang.IntKey(i2(t)), s)
        case n => n
      }
      Local(replacedName, assigned.replace(i, i2), body.replace(i, i2))
    }
    def replaceDelayed(i: DelayedTableSchema): Expr[R] = {
      val replacedName = name match {
        case d: Delayed => i.replace(d)
        case n          => n
      }
      Local(replacedName, assigned.replaceDelayed(i), body.replaceDelayed(i))
    }

  }

}

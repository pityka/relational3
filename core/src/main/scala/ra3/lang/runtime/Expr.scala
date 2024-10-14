package ra3.lang.runtime

import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.tablelang.DelayedTableSchema
import ra3.tablelang
import ra3.*
import ra3.lang.{Key, KeyTag, Delayed, ColumnKey, TagKey, IntKey, ops}

private[ra3] case class Value(v: Any)

private[ra3] sealed trait Expr { self =>

  protected def evalWith(env: Map[Key, Value])(implicit
      tsc: TaskSystemComponents
  ): IO[Value]
  private[ra3] def hash = {
    val bytes = writeToArray(this)
    com.google.common.hash.Hashing.murmur3_128().hashBytes(bytes).asLong()
  }

  private[ra3] def columnKeys: Set[ColumnKey]

}
object Expr {

  private[ra3] def evaluate(expr: Expr, map: Map[Key, Value])(implicit
      tsc: TaskSystemComponents
  ): IO[Value] = expr.evalWith(map)

  implicit val customCodecOfDouble: JsonValueCodec[Double] =
  ra3.Utils.customDoubleCodec

  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[Expr] =
  JsonCodecMaker.make(CodecMakerConfig.withAllowRecursiveTypes(true))
  // $COVERAGE-ON$

  final case class DelayedIdent private[ra3] (private[ra3] val name: Delayed)
      extends Expr { self =>

    protected def evalWith(env: Map[Key, Value])(implicit
        tsc: TaskSystemComponents
    ): IO[Value] =
      IO.pure(env(name))

    private[ra3] val columnKeys: Set[ColumnKey] = Set.empty

  }
  private[ra3] final case class Ident(name: Key) extends Expr {

    def evalWith(env: Map[Key, Value])(implicit
        tsc: TaskSystemComponents
    ) =
      IO.pure(env(name))

    val columnKeys: Set[ColumnKey] = name match {
      case a: ColumnKey => Set(a)
      case _            => Set.empty
    }

  }

  private[ra3] final case class BuiltInOp0(op: ops.Op0) extends Expr {

    private[ra3] def referredTables = Set.empty
    val columnKeys: Set[ColumnKey] = Set.empty

    def evalWith(
        env: Map[Key, Value]
    )(implicit tsc: TaskSystemComponents) =
      IO.pure(Value(op.op()))

  }
  private[ra3] final case class BuiltInOp1(op: ops.Op1, arg0: Expr)
      extends Expr {

    val columnKeys: Set[ColumnKey] = arg0.columnKeys

    def evalWith(
        env: Map[Key, Value]
    )(implicit tsc: TaskSystemComponents): IO[Value] =
      arg0
        .evalWith(env)
        .flatMap(v => op.op(v.v.asInstanceOf[op.A0]).map(Value(_)))

  }
  private[ra3] final case class BuiltInOp2(
      op: ops.Op2,
      val arg0: Expr,
      val arg1: Expr
  ) extends Expr {

    val columnKeys: Set[ColumnKey] = arg0.columnKeys ++ arg1.columnKeys

    def evalWith(
        env: Map[Key, Value]
    )(implicit tsc: TaskSystemComponents): IO[Value] =
      for {
        a0 <- arg0.evalWith(env)
        a1 <- arg1.evalWith(env)
        r <- op.op(a0.v.asInstanceOf[op.A0], a1.v.asInstanceOf[op.A1])
      } yield Value(r)

  }
  private[ra3] final case class BuiltInOp3(
      op: ops.Op3,
      val arg0: Expr,
      val arg1: Expr,
      val arg2: Expr
  ) extends Expr {

    val columnKeys: Set[ColumnKey] =
      arg0.columnKeys ++ arg1.columnKeys ++ arg2.columnKeys

    def evalWith(
        env: Map[Key, Value]
    )(implicit tsc: TaskSystemComponents): IO[Value] =
      for {
        a0 <- arg0.evalWith(env)
        a1 <- arg1.evalWith(env)
        a2 <- arg2.evalWith(env)
        r <- op.op(
          a0.v.asInstanceOf[op.A0],
          a1.v.asInstanceOf[op.A1],
          a2.v.asInstanceOf[op.A2]
        )
      } yield Value(r)

  }

  private[ra3] final case class BuiltInOpAny(op: ops.OpAny, val args: Seq[Expr])
      extends Expr {

    val columnKeys: Set[ColumnKey] =
      args.flatMap(_.columnKeys).toSet

    def evalWith(
        env: Map[Key, Value]
    )(implicit tsc: TaskSystemComponents): IO[Value] =
      IO.parSequenceN(32)(
        args.map(_.evalWith(env))
      ).flatMap(args => op.op(args.toList.map(_.v.asInstanceOf[op.A])))
        .map(value => Value(value))

  }

  private[ra3] final case class Local(name: Key, assigned: Expr, body: Expr)
      extends Expr {
    self =>

    val columnKeys: Set[ColumnKey] = assigned.columnKeys ++ body.columnKeys

    def evalWith(
        env: Map[Key, Value]
    )(implicit tsc: TaskSystemComponents): IO[Value] = {
      assigned.evalWith(env).flatMap { assignedValue =>
        body.evalWith(env + (name -> assignedValue))
      }
    }

  }

}

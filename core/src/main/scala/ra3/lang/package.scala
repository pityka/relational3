package ra3
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.tablelang.TableExpr
package object lang {

  type Query = ra3.lang.Expr { type T <: ra3.lang.ReturnValue }
  type IntExpr = Expr { type T = Int }
  type StrExpr = Expr { type T = String }
  type BufferExpr = Expr { type T <: Buffer }
  type BufferIntExpr = Expr { type T = BufferInt }

  type DI32 = Either[BufferInt, Seq[SegmentInt]]
  type DI64 = Either[BufferLong, Seq[SegmentLong]]
  type DF64 = Either[BufferDouble, Seq[SegmentDouble]]
  type DStr = Either[BufferString, Seq[SegmentString]]
  type DInst = Either[BufferInstant, Seq[SegmentInstant]]

  type ColumnExpr = Expr {
    type T = Either[Buffer, Seq[Segment]]
  }
  type I32ColumnExpr = Expr {
    type T = DI32
  }
  type I64ColumnExpr = Expr {
    type T = DI64
  }
  type F64ColumnExpr = Expr {
    type T = Either[BufferDouble, Seq[SegmentDouble]]
  }
  type StrColumnExpr = Expr {
    type T = Either[BufferString, Seq[SegmentString]]
  }
  type InstColumnExpr = Expr {
    type T = Either[BufferInstant, Seq[SegmentInstant]]
  }

  type ReturnExpr = Expr { type T = ReturnValue }

  type GenericExpr[T0] = Expr { type T = T0 }
  type DelayedIdent[T0] = Expr.DelayedIdent { type T = T0 }

  implicit class SyntaxExpr[T0](a: Expr { type T = T0 })
      extends syntax.SyntaxExprImpl[T0] {
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
  implicit class SyntaxColumn(a: ColumnExpr) extends syntax.SyntaxColumnImpl {
    protected def arg0 = a

  }

  implicit class SyntaxInt(a: IntExpr) extends syntax.SyntaxIntImpl {
    protected def arg0 = a

  }
  implicit class SyntaxIntLit(a: Int) extends syntax.SyntaxIntImpl {
    protected def arg0 = Expr.LitNum(a)
    def lift = arg0
  }
  implicit class SyntaxStrLit(a: String) {
    protected def arg0 = Expr.LitStr(a)
    def lift = arg0
  }
  import scala.language.implicitConversions
  implicit def conversionF64Lit(a: Double): Expr.LitF64 = Expr.LitF64(a)
  implicit def conversionF64LitSet(a: Set[Double]): Expr.LitF64Set =
    Expr.LitF64Set(a)
  implicit def conversionI32LitSet(a: Set[Int]): Expr.LitI32Set =
    Expr.LitI32Set(a)
  implicit def conversionIntLit(a: Int): Expr.LitNum = Expr.LitNum(a)
  implicit def conversionStrLit(a: String): Expr.LitStr = Expr.LitStr(a)
  implicit def conversionStrSetLit(a: Set[String]): Expr.LitStrSet =
    Expr.LitStrSet(a)
  implicit def conversionDI32(
      a: Expr { type T = DI32 }
  ): Expr { type T = ColumnSpec } = a.unnamed

  implicit def conversionDF64(
      a: Expr { type T = DF64 }
  ): Expr { type T = ColumnSpec } = a.unnamed

  implicit def conversionStr64(
      a: Expr { type T = DStr }
  ): Expr { type T = ColumnSpec } = a.unnamed

  class Identifier[T0](id: Expr { type T = T0 }) {
    def apply[T1](
        body: Expr { type T = T0 } => Expr { type T = T1 }
    ): Expr { type T = T1 } =
      local(id)(body)
  }

  val star: Expr { type T = ColumnSpec } = ra3.lang.Expr.Star

  def local[T1](assigned: Expr)(body: Expr { type T = assigned.T } => Expr {
    type T = T1
  }): Expr { type T = T1 } = {
    val n = TagKey(new KeyTag)
    val b = body(Expr.Ident(n).as[assigned.T])

    Expr.Local(n, assigned, b).asInstanceOf[Expr { type T = T1 }]
  }
  def let[T1](assigned: Expr)(body: Expr { type T = assigned.T } => Expr {
    type T = T1
  }): Expr { type T = T1 } =
    local(assigned)(body)

  private[ra3] def global[T0](n: ColumnKey): Identifier[T0] = {
    val id = Expr.Ident(n).as[T0]
    new Identifier(id)
  }

  def select(args: Expr { type T = ColumnSpec }*): ReturnExpr = {
    Expr.makeOpStar(ops.MkSelect)(args: _*)
  }

  private[ra3] def evaluate(expr: Expr)(implicit
      tsc: TaskSystemComponents
  ): IO[Value[expr.T]] = expr.evalWith(Map.empty)
  private[ra3] def evaluate(expr: Expr, map: Map[Key, Value[_]])(implicit
      tsc: TaskSystemComponents
  ): IO[Value[expr.T]] =
    expr.evalWith(map)

  private[ra3] def bufferIfNeeded[
      B <: Buffer { type BufferType = B },
      S <: Segment { type SegmentType = S; type BufferType = B },
      C
  ](
      arg: Either[B, Seq[S]]
  )(implicit tsc: TaskSystemComponents): IO[B] =
    arg match {
      case Left(b) => IO.pure(b)
      case Right(s) =>
        ra3.Utils.bufferMultiple(s)
    }
  private[ra3] def bufferIfNeededWithPrecondition[
      B <: Buffer { type BufferType = B },
      S <: Segment { type SegmentType = S; type BufferType = B },
      C
  ](
      arg: Either[B, Seq[S]]
  )(prec: S => Boolean)(implicit tsc: TaskSystemComponents): IO[Either[Int,B]] =
    arg match {
      case Left(b) => IO.pure(Right(b))
      case Right(s) =>
        if (s.exists(prec))
        ra3.Utils.bufferMultiple(s).map(Right(_))
        else IO.pure(Left(s.map(_.numElems).sum))
    }

  // *****

  object Join {
    def apply(a: Expr.DelayedIdent) =
      JoinBuilderSyntax(a, Vector.empty, None, None, None, None)
  }
  object GroupBy {
    def apply(a: Expr.DelayedIdent) =
      GroupBuilderSyntax(a, Vector.empty, None, None, None, None, false)
  }

  def local[T1](
      assigned: TableExpr
  )(body: TableExpr.Ident => TableExpr): TableExpr = {
    val n = ra3.tablelang.TagKey(new ra3.tablelang.KeyTag)
    val b = body(TableExpr.Ident(n))

    TableExpr.Local(n, assigned, b)
  }
  def useTable(assigned: ra3.Table)(
      body: TableExpr.Ident => TableExpr
  ): TableExpr =
    local(TableExpr.Const(assigned))(body)
  def schema[T0: NotNothing](assigned: ra3.Table)(
      body: (TableExpr.Ident, ra3.lang.DelayedIdent[T0]) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(assigned)) { t =>
      t.useColumn[T0](0) { c0 =>
        body(t, c0)
      }
    }
  }
  def schema[T0: NotNothing, T1: NotNothing](assigned: ra3.Table)(
      body: (
          TableExpr.Ident,
          ra3.lang.DelayedIdent[T0],
          ra3.lang.DelayedIdent[T1]
      ) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(assigned)) { t =>
      t.useColumns[T0, T1](0, 1) { case (c0, c1) =>
        body(t, c0, c1)
      }
    }
  }
  def schema[T0: NotNothing, T1: NotNothing, T2: NotNothing](
      assigned: ra3.Table
  )(
      body: (
          TableExpr.Ident,
          ra3.lang.DelayedIdent[T0],
          ra3.lang.DelayedIdent[T1],
          ra3.lang.DelayedIdent[T2]
      ) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(assigned)) { t =>
      t.useColumns[T0, T1, T2](0, 1, 2) { case (c0, c1, c2) =>
        body(t, c0, c1, c2)
      }
    }
  }
  def schema[T0: NotNothing, T1: NotNothing, T2: NotNothing, T3: NotNothing](
      assigned: ra3.Table
  )(
      body: (
          TableExpr.Ident,
          ra3.lang.DelayedIdent[T0],
          ra3.lang.DelayedIdent[T1],
          ra3.lang.DelayedIdent[T2],
          ra3.lang.DelayedIdent[T3]
      ) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(assigned)) { t =>
      t.useColumns[T0, T1, T2, T3](0, 1, 2, 3) { case (c0, c1, c2, c3) =>
        body(t, c0, c1, c2, c3)
      }
    }
  }

  case class GroupBuilderSyntax(
      private val first: Expr.DelayedIdent,
      private val others: Vector[
        (Expr.DelayedIdent)
      ],
      private val prg: Option[ra3.lang.Query],
      private val partitionBase: Option[Int],
      private val partitionLimit: Option[Int],
      private val maxSegmentsToBufferAtOnce: Option[Int],
      private val _partial: Boolean
  ) {

    def by(n: Expr.DelayedIdent) = {
      require(first.name.table == n.name.table)
      copy(others = others :+ n)
    }

    def partial = copy(_partial = true)

    def withPartitionBase(num: Int) = copy(partitionBase = Some(num))
    def withPartitionLimit(num: Int) = copy(partitionLimit = Some(num))
    def withMaxSegmentsBufferingAtOnce(num: Int) =
      copy(maxSegmentsToBufferAtOnce = Some(num))
    def reduceGroupsWith(q: ra3.lang.Query) = copy(prg = Some(q)).done
    def done = if (!_partial)
      ra3.tablelang.TableExpr.GroupThenReduce(
        first,
        others,
        prg.getOrElse(ra3.lang.select(ra3.lang.star)),
        partitionBase.getOrElse(128),
        partitionLimit.getOrElse(10_000_000),
        maxSegmentsToBufferAtOnce.getOrElse(10)
      )
    else
      ra3.tablelang.TableExpr.GroupPartialThenReduce(
        first,
        others,
        prg.getOrElse(ra3.lang.select(ra3.lang.star))
      )

  }
  implicit def convertJoinBuilder(j: JoinBuilderSyntax): TableExpr = j.done
  case class JoinBuilderSyntax(
      private val first: Expr.DelayedIdent,
      private val others: Vector[
        (Expr.DelayedIdent, String, ra3.tablelang.Key)
      ],
      private val prg: Option[ra3.lang.Query],
      private val partitionBase: Option[Int],
      private val partitionLimit: Option[Int],
      private val maxSegmentsToBufferAtOnce: Option[Int]
  ) {
    def withMaxSegmentsBufferingAtOnce(num: Int) =
      copy(maxSegmentsToBufferAtOnce = Some(num))

    def inner(ref: Expr.DelayedIdent, other: TableExpr.Ident) = {
      copy(
        others = others.appended(
          (ref, "inner", other.key)
        )
      )
    }
    def inner(ref: Expr.DelayedIdent) = {
      copy(
        others = others.appended(
          (ref, "inner", first.name.table)
        )
      )
    }
    def outer(ref: Expr.DelayedIdent, other: TableExpr.Ident) = {
      copy(
        others = others.appended(
          (ref, "outer", other.key)
        )
      )
    }
    def outer(ref: Expr.DelayedIdent) = {
      copy(
        others = others.appended(
          (ref, "outer", first.name.table)
        )
      )
    }
    def left(ref: Expr.DelayedIdent, other: TableExpr.Ident) = {
      copy(
        others = others.appended(
          (ref, "left", other.key)
        )
      )
    }
    def left(ref: Expr.DelayedIdent) = {
      copy(
        others = others.appended(
          (ref, "left", first.name.table)
        )
      )
    }
    def right(ref: Expr.DelayedIdent, other: TableExpr.Ident) = {
      copy(
        others = others.appended(
          (ref, "right", other.key)
        )
      )
    }
    def right(ref: Expr.DelayedIdent) = {
      copy(
        others = others.appended(
          (ref, "right", first.name.table)
        )
      )
    }
    def withPartitionBase(num: Int) = copy(partitionBase = Some(num))
    def withPartitionLimit(num: Int) = copy(partitionLimit = Some(num))
    def elementwise(q: ra3.lang.Query) = copy(prg = Some(q)).done
    def done = {
      ra3.tablelang.TableExpr.Join(
        first,
        others,
        partitionBase.getOrElse(128),
        partitionLimit.getOrElse(10_000_000),
        maxSegmentsToBufferAtOnce.getOrElse(10),
        prg.getOrElse(ra3.lang.select(ra3.lang.star))
      )
    }
  }

  implicit class SyntaxTableExpr(a: TableExpr) {
    def in0(body: TableExpr.Ident => TableExpr): TableExpr =
      local(a)(i => body(i))

    def in[T0: NotNothing](
        body: (TableExpr.Ident, ra3.lang.DelayedIdent[T0]) => TableExpr
    ): TableExpr = {
      local(a) { t =>
        t.useColumn[T0](0) { c0 =>
          body(t, c0)
        }
      }
    }
    def in[T0: NotNothing, T1: NotNothing](
        body: (
            TableExpr.Ident,
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1]
        ) => TableExpr
    ): TableExpr = {
      local(a) { t =>
        t.useColumns[T0, T1](0, 1) { case (c0, c1) =>
          body(t, c0, c1)
        }
      }
    }
    def in[T0: NotNothing, T1: NotNothing, T2: NotNothing](
        body: (
            TableExpr.Ident,
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1],
            ra3.lang.DelayedIdent[T2]
        ) => TableExpr
    ): TableExpr = {
      local(a) { t =>
        t.useColumns[T0, T1, T2](0, 1, 2) { case (c0, c1, c2) =>
          body(t, c0, c1, c2)
        }
      }
    }
    def in[T0: NotNothing, T1: NotNothing, T2: NotNothing, T3: NotNothing](
        body: (
            TableExpr.Ident,
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1],
            ra3.lang.DelayedIdent[T2],
            ra3.lang.DelayedIdent[T3]
        ) => TableExpr
    ): TableExpr = {
      local(a) { t =>
        t.useColumns[T0, T1, T2, T3](0, 1, 2, 3) { case (c0, c1, c2, c3) =>
          body(t, c0, c1, c2, c3)
        }
      }
    }
  }

  implicit class SyntaxTableExprIdent(a: TableExpr.Ident) {

    def reduce(
        prg: ra3.lang.Query
    ) =
      ra3.tablelang.TableExpr.ReduceTable(
        arg0 = a,
        groupwise = prg
      )
    def partialReduce(
        prg: ra3.lang.Query
    ) =
      ra3.tablelang.TableExpr.FullTablePartialReduce(
        arg0 = a,
        groupwise = prg
      )

    def query(prg: ra3.lang.Query) =
      ra3.tablelang.TableExpr.SimpleQuery(a, prg)

    @scala.annotation.nowarn
    def useColumn[T0: NotNothing](
        n1: String
    )(
        body: (
            ra3.lang.DelayedIdent[T0]
        ) => TableExpr
    ) = {
      val d1 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Left(n1))).cast[T0]
      body(d1)
    }
    @scala.annotation.nowarn
    def useColumn[T0: NotNothing](
        n1: Int
    )(
        body: (
            ra3.lang.DelayedIdent[T0]
        ) => TableExpr
    ) = {
      val d1 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n1))).cast[T0]
      body(d1)
    }

    @scala.annotation.nowarn
    def useColumns[T0: NotNothing, T1: NotNothing](
        n1: String,
        n2: String
    )(
        body: (
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1]
        ) => TableExpr
    ) = {
      val d1 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Left(n1))).cast[T0]
      val d2 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Left(n2))).cast[T1]
      body(d1, d2)
    }
    @scala.annotation.nowarn
    def useColumns[T0: NotNothing, T1: NotNothing, T2: NotNothing](
        n1: String,
        n2: String,
        n3: String
    )(
        body: (
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1],
            ra3.lang.DelayedIdent[T2]
        ) => TableExpr
    ) = {
      val d1 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Left(n1))).cast[T0]
      val d2 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Left(n2))).cast[T1]
      val d3 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Left(n3))).cast[T2]
      body(d1, d2, d3)
    }
    @scala.annotation.nowarn
    def useColumns[T0: NotNothing, T1: NotNothing](
        n1: Int,
        n2: Int
    )(
        body: (
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1]
        ) => TableExpr
    ) = {
      val d1 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n1))).cast[T0]
      val d2 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n2))).cast[T1]
      body(d1, d2)
    }

    @scala.annotation.nowarn
    def useColumns[T0: NotNothing, T1: NotNothing, T2: NotNothing](
        n1: Int,
        n2: Int,
        n3: Int
    )(
        body: (
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1],
            ra3.lang.DelayedIdent[T2]
        ) => TableExpr
    ) = {
      val d1 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n1))).cast[T0]
      val d2 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n2))).cast[T1]
      val d3 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n3))).cast[T2]
      body(d1, d2, d3)
    }

    @scala.annotation.nowarn
    def useColumns[
        T0: NotNothing,
        T1: NotNothing,
        T2: NotNothing,
        T3: NotNothing
    ](
        n1: Int,
        n2: Int,
        n3: Int,
        n4: Int
    )(
        body: (
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1],
            ra3.lang.DelayedIdent[T2],
            ra3.lang.DelayedIdent[T3]
        ) => TableExpr
    ) = {
      val d1 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n1))).cast[T0]
      val d2 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n2))).cast[T1]
      val d3 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n3))).cast[T2]
      val d4 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n4))).cast[T3]
      body(d1, d2, d3, d4)
    }

  }
}

package ra3.tablelang
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.GroupedTable
import ra3.lang.Expr
import ra3.NotNothing

private[ra3] class KeyTag
private[ra3] sealed trait Key
private[ra3] case class IntKey(s: Int) extends Key
private[ra3] case class TagKey(s: KeyTag) extends Key
private[ra3] case class TableKey(tableUniqueId: String) extends Key

private[ra3] case class DelayedTableSchema(
    map: Map[Key, (String, Seq[String])]
) {
  private def findIdx(n: String, colNames: Seq[String]) = colNames.zipWithIndex
    .find(_._1 == n)
    .getOrElse(throw new NoSuchElementException(s"column $n not found"))
    ._2
  def replace(delayed: ra3.lang.Delayed) = {
    val (tableUniqueId, colNames) = map(delayed.table)
    val idx = delayed.selection match {
      case Left(i)  => findIdx(i, colNames)
      case Right(i) => i
    }
    ra3.lang.ColumnKey(tableUniqueId, idx)
  }
}

sealed trait TableExpr { self =>

  def evaluate(implicit
      tsc: TaskSystemComponents
  ): IO[ra3.Table] = evalWith(Map.empty).map(_.v)

  private[tablelang] def evalWith(env: Map[Key, TableValue])(implicit
      tsc: TaskSystemComponents
  ): IO[TableValue]

  private[ra3] def hash = {
    val bytes = writeToArray(this.replaceTags())
    com.google.common.hash.Hashing.murmur3_128().hashBytes(bytes).asLong()
  }
  // utilities for analysis of the tree
  private[tablelang] def tags: Set[KeyTag]
  private[tablelang] def replace(map: Map[KeyTag, Int]): TableExpr
  private[ra3] def replaceTags() =
    replace(this.tags.toSeq.zipWithIndex.toMap)

}
private[ra3] object TableExpr {
  import scala.language.implicitConversions
  implicit def convertJoinBuilder(j: ra3.lang.JoinBuilderSyntax): TableExpr =
    j.done

  implicit class SyntaxTableExpr(a: TableExpr) {
    def in0(body: TableExpr.Ident => TableExpr): TableExpr =
      ra3.lang.local(a)(i => body(i))

    def in[T0: NotNothing](
        body: (ra3.lang.DelayedIdent[T0]) => TableExpr
    ): TableExpr = {
      ra3.lang.local(a) { t =>
        t.useColumn[T0](0) { c0 =>
          body(c0)
        }
      }
    }
    def in[T0: NotNothing, T1: NotNothing](
        body: (
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1]
        ) => TableExpr
    ): TableExpr = {
      ra3.lang.local(a) { t =>
        t.useColumns[T0, T1](0, 1) { case (c0, c1) =>
          body(c0, c1)
        }
      }
    }
    def in[T0: NotNothing, T1: NotNothing, T2: NotNothing](
        body: (
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1],
            ra3.lang.DelayedIdent[T2]
        ) => TableExpr
    ): TableExpr = {
      ra3.lang.local(a) { t =>
        t.useColumns[T0, T1, T2](0, 1, 2) { case (c0, c1, c2) =>
          body(c0, c1, c2)
        }
      }
    }
    def in[T0: NotNothing, T1: NotNothing, T2: NotNothing, T3: NotNothing](
        body: (
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1],
            ra3.lang.DelayedIdent[T2],
            ra3.lang.DelayedIdent[T3]
        ) => TableExpr
    ): TableExpr = {
      ra3.lang.local(a) { t =>
        t.useColumns[T0, T1, T2, T3](0, 1, 2, 3) { case (c0, c1, c2, c3) =>
          body(c0, c1, c2, c3)
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
    def count(prg: ra3.lang.Query) =
      ra3.tablelang.TableExpr.SimpleQueryCount(a, prg)

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
    @scala.annotation.nowarn
    def useColumns[
        T0: NotNothing,
        T1: NotNothing,
        T2: NotNothing,
        T3: NotNothing,
        T4: NotNothing
    ](
        n1: Int,
        n2: Int,
        n3: Int,
        n4: Int,
        n5: Int
    )(
        body: (
            ra3.lang.DelayedIdent[T0],
            ra3.lang.DelayedIdent[T1],
            ra3.lang.DelayedIdent[T2],
            ra3.lang.DelayedIdent[T3],
            ra3.lang.DelayedIdent[T4]
        ) => TableExpr
    ) = {
      val d1 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n1))).cast[T0]
      val d2 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n2))).cast[T1]
      val d3 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n3))).cast[T2]
      val d4 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n4))).cast[T3]
      val d5 = Expr.DelayedIdent(ra3.lang.Delayed(a.key, Right(n5))).cast[T4]
      body(d1, d2, d3, d4, d5)
    }

  }

  implicit val codec: JsonValueCodec[TableExpr] =
    JsonCodecMaker.make(CodecMakerConfig.withAllowRecursiveTypes(true))

  case class Const(table: ra3.Table) extends TableExpr {
    type T = TableValue

    def evalWith(env: Map[Key, TableValue])(implicit
        tsc: TaskSystemComponents
    ) =
      IO.pure(TableValue(table))

    def tags: Set[KeyTag] = Set.empty

    override def replace(i: Map[KeyTag, Int]) = this
  }
  case class Ident(private[ra3] key: Key) extends TableExpr {

    private[ra3] def evalWith(env: Map[Key, TableValue])(implicit
        tsc: TaskSystemComponents
    ) =
      IO.pure(env(key))

    private[ra3] def tags: Set[KeyTag] = key match {
      case TagKey(s) => Set(s)
      case _         => Set.empty
    }

    private[ra3] override def replace(i: Map[KeyTag, Int]) = key match {
      case TagKey(s) => Ident(IntKey(i(s)))
      case _         => this
    }
  }

  case class Local(name: Key, assigned: TableExpr, body: TableExpr)
      extends TableExpr {
    self =>

    val tags: Set[KeyTag] = name match {
      case TagKey(s) => Set(s) ++ assigned.tags ++ body.tags
      case _         => assigned.tags ++ body.tags
    }

    def replace(i: Map[KeyTag, Int]): TableExpr = {
      val replacedName = name match {
        case TagKey(t) => IntKey(i(t))
        case n         => n
      }
      Local(replacedName, assigned.replace(i), body.replace(i))
    }
    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      assigned.evalWith(env).flatMap { assignedValue =>
        body.evalWith(env + (name -> assignedValue))
      }
    }

  }

  case class SimpleQuery(
      arg0: TableExpr.Ident,
      elementwise: ra3.lang.Expr
  ) extends TableExpr {

    val tags: Set[KeyTag] = arg0.tags
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      SimpleQuery(
        arg0.replace(i).asInstanceOf[Ident],
        elementwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          scribe.info(
            f"Will do simpl query on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments)"
          )
          ra3.SimpleQuery
            .simpleQuery(
              table,
              elementwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(arg0.key -> (table.uniqueId, table.colNames))
                  )
                )
                .asInstanceOf[ra3.lang.Query]
            )
            .map(t => TableValue(t))

        }

    }
  }
  case class SimpleQueryCount(
      arg0: TableExpr.Ident,
      elementwise: ra3.lang.Expr
  ) extends TableExpr {

    val tags: Set[KeyTag] = arg0.tags
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      SimpleQueryCount(
        arg0.replace(i).asInstanceOf[Ident],
        elementwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          scribe.info(
            f"Will do simple query count on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments)"
          )
          ra3.SimpleQuery
            .simpleQueryCount(
              table,
              elementwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(arg0.key -> (table.uniqueId, table.colNames))
                  )
                )
                .asInstanceOf[ra3.lang.Query]
            )
            .map(t => TableValue(t))

        }

    }
  }
  case class GroupThenReduce(
      arg0: ra3.lang.Expr.DelayedIdent,
      arg1: Seq[(ra3.lang.Expr.DelayedIdent)],
      groupwise: ra3.lang.Expr,
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int
  ) extends TableExpr {

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      GroupThenReduce(
        arg0.replace(Map.empty, i).asInstanceOf[ra3.lang.Expr.DelayedIdent],
        arg1.map(
          _.replace(Map.empty, i).asInstanceOf[ra3.lang.Expr.DelayedIdent]
        ),
        groupwise.replaceTags(i),
        partitionBase,
        partitionLimit,
        maxSegmentsToBufferAtOnce
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0.tableIdent
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          val cols = (arg0 +: arg1).map(
            _.name.selection.left
              .map(s => table.colNames.indexOf(s))
              .fold(identity, identity)
          )
          scribe.info(
            f"Will do group by on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments) on ${cols.size} columns"
          )
          table
            .groupBy(
              cols = cols,
              partitionBase = partitionBase,
              partitionLimit = partitionLimit,
              maxSegmentsToBufferAtOnce = maxSegmentsToBufferAtOnce
            )
            .flatMap { groupedTable =>
              scribe.info(
                f"Will do reduction on ${groupedTable.uniqueId} (${groupedTable.partitions.size} partitions) "
              )
              val program = groupwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(
                      arg0.name.table -> (groupedTable.uniqueId, groupedTable.colNames)
                    )
                  )
                )
                .asInstanceOf[ra3.lang.Query]
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class GroupThenCount(
      arg0: ra3.lang.Expr.DelayedIdent,
      arg1: Seq[(ra3.lang.Expr.DelayedIdent)],
      groupwise: ra3.lang.Expr,
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int
  ) extends TableExpr {

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      GroupThenCount(
        arg0.replace(Map.empty, i).asInstanceOf[ra3.lang.Expr.DelayedIdent],
        arg1.map(
          _.replace(Map.empty, i).asInstanceOf[ra3.lang.Expr.DelayedIdent]
        ),
        groupwise.replaceTags(i),
        partitionBase,
        partitionLimit,
        maxSegmentsToBufferAtOnce
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0.tableIdent
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          val cols = (arg0 +: arg1).map(
            _.name.selection.left
              .map(s => table.colNames.indexOf(s))
              .fold(identity, identity)
          )
          scribe.info(
            f"Will do group by on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments) on ${cols.size} columns"
          )
          table
            .groupBy(
              cols = cols,
              partitionBase = partitionBase,
              partitionLimit = partitionLimit,
              maxSegmentsToBufferAtOnce = maxSegmentsToBufferAtOnce
            )
            .flatMap { groupedTable =>
              scribe.info(
                f"Will do reduction on ${groupedTable.uniqueId} (${groupedTable.partitions.size} partitions) "
              )
              val program = groupwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(
                      arg0.name.table -> (groupedTable.uniqueId, groupedTable.colNames)
                    )
                  )
                )
                .asInstanceOf[ra3.lang.Query]
              GroupedTable.countGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class GroupPartialThenReduce(
      arg0: ra3.lang.Expr.DelayedIdent,
      arg1: Seq[(ra3.lang.Expr.DelayedIdent)],
      groupwise: ra3.lang.Expr
  ) extends TableExpr {

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      GroupPartialThenReduce(
        arg0.replace(Map.empty, i).asInstanceOf[ra3.lang.Expr.DelayedIdent],
        arg1.map(
          _.replace(Map.empty, i).asInstanceOf[ra3.lang.Expr.DelayedIdent]
        ),
        groupwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0.tableIdent
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          val cols = (arg0 +: arg1).map(
            _.name.selection.left
              .map(s => table.colNames.indexOf(s))
              .fold(identity, identity)
          )
          scribe.info(
            f"Will do partial group by segments  on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments) on ${cols.size} columns"
          )
          table
            .groupBySegments(
              cols = cols
            )
            .flatMap { groupedTable =>
              scribe.info(
                f"Will do partial reduction on ${groupedTable.uniqueId} (${groupedTable.partitions.size} partitions) "
              )
              val program = groupwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(
                      arg0.name.table -> (groupedTable.uniqueId, groupedTable.colNames)
                    )
                  )
                )
                .asInstanceOf[ra3.lang.Query]
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class FullTablePartialReduce(
      arg0: TableExpr.Ident,
      groupwise: ra3.lang.Expr
  ) extends TableExpr {

    val tags: Set[KeyTag] = arg0.tags
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      FullTablePartialReduce(
        arg0.replace(i).asInstanceOf[Ident],
        groupwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          ra3.ReduceTable
            .formSingleGroupAsOnePartitionPerSegment(table)
            .flatMap { groupedTable =>
              scribe.info(
                f"Will do partial reduction on ${groupedTable.uniqueId} (${groupedTable.partitions.size} partitions) "
              )

              val program = groupwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(
                      arg0.key -> (groupedTable.uniqueId, groupedTable.colNames)
                    )
                  )
                )
                .asInstanceOf[ra3.lang.Query]
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class ReduceTable(
      arg0: TableExpr.Ident,
      groupwise: ra3.lang.Expr
  ) extends TableExpr {

    val tags: Set[KeyTag] = arg0.tags
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      ReduceTable(
        arg0.replace(i).asInstanceOf[Ident],
        groupwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          scribe.info(
            f"Will do full table reduction at once  on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments) "
          )

          ra3.ReduceTable
            .formSingleGroup(table)
            .flatMap { groupedTable =>
              val program = groupwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(
                      arg0.key -> (groupedTable.uniqueId, groupedTable.colNames)
                    )
                  )
                )
                .asInstanceOf[ra3.lang.Query]
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class Join(
      arg0: ra3.lang.Expr.DelayedIdent,
      arg1: Seq[(ra3.lang.Expr.DelayedIdent, String, ra3.tablelang.Key)],
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int,
      elementwise: ra3.lang.Expr
  ) extends TableExpr {

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_._1.tableIdent.tags)
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      Join(
        arg0.replace(Map.empty, i).asInstanceOf[Expr.DelayedIdent],
        arg1.map { case (columnAndTable, how, against) =>
          (
            columnAndTable
              .replace(Map.empty, i)
              .asInstanceOf[Expr.DelayedIdent],
            how,
            against
          )
        },
        partitionBase,
        partitionLimit,
        maxSegmentsToBufferAtOnce,
        elementwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0.tableIdent
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          IO.parSequenceN(32)(arg1.map { case (a, b, c) =>
            a.tableIdent.evalWith(env).map(x => (x.v, a, b, c))
          }).flatMap { arg1Tables =>
            val program = elementwise
              .replaceDelayed(
                DelayedTableSchema(
                  Map(
                    arg0.name.table -> ((table.uniqueId, table.colNames))
                  ) ++ (arg1Tables.map(_._1) zip arg1.map(_._1)).map {
                    case (table, argxDelayed) =>
                      (argxDelayed.name.table, (table.uniqueId, table.colNames))
                  }
                )
              )
              .asInstanceOf[ra3.lang.Query]

            val firstColIdx = arg0.name.selection.left
              .map(s => table.colNames.indexOf(s))
              .fold(identity, identity)

            def tableStrings = (List(table) ++ arg1Tables.map(_._1)).map {
              table =>
                f"${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments)  "
            }

            scribe.info(
              f"Will join the following tables: ${tableStrings.mkString(" x ")} "
            )

            ra3.Equijoin
              .equijoinPlanner(
                table,
                firstColIdx,
                arg1Tables.map {
                  case (table, delayedCol, how, delayedAgainst) =>
                    val colIdx: Int = delayedCol.name.selection.left
                      .map(s => table.colNames.indexOf(s))
                      .fold(identity, identity)
                    val againstTable: Int =
                      if (arg0.name.table == delayedAgainst) 0
                      else {
                        val c = arg1.zipWithIndex
                          .find(_._1._1.name.table == delayedAgainst)
                          .get
                          ._2
                        c + 1
                      }

                    (table, colIdx, how, againstTable)
                },
                partitionBase,
                partitionLimit,
                maxSegmentsToBufferAtOnce,
                program
              )
              .map(TableValue(_))
          }

        }

    }
  }
}

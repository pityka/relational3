package ra3.tablelang
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.GroupedTable
import ra3.lang.Expr
import ra3.NotNothing
import ra3.lang.Expr.DelayedIdent
import ra3.lang.ReturnValue
import ra3.lang.ReturnValueTuple

sealed trait TableExpr[+T] { self =>

  def render: String = Render.render(self, 0)

  def evaluate(implicit
      tsc: TaskSystemComponents
  ): IO[ra3.Table] = evalWith(Map.empty).map(_.v)

   def evalWith(env: Map[Key, TableValue])(implicit
      tsc: TaskSystemComponents
  ): IO[TableValue]

  // utilities for analysis of the tree
   def tags: Set[KeyTag]

   def in[R](
      body: TableExpr.Ident[T] => TableExpr[R]
  ): TableExpr[R] = {
    val n = ra3.tablelang.TagKey(new ra3.tablelang.KeyTag)
    val b = body(TableExpr.Ident(n))
    TableExpr.Local(n, self, b)
  }

}
object TableExpr {

  case class curryColumnsTuple[T0 <: Tuple](
      a: TableExpr[ra3.lang.ReturnValueTuple[T0]]
  ) {
    inline def apply[R](
        inline body: (Schema[T0]) => TableExpr[R]
    ): TableExpr[R] =
      a.in { (t: Ident[ReturnValueTuple[T0]]) =>
        body(Schema.fromIdent(t))
      }
    inline def all[R](
        inline body: Tuple.Map[T0, ra3.lang.Expr.DelayedIdent] => TableExpr[R]
    ): TableExpr[R] =
      a.in { (t: Ident[ReturnValueTuple[T0]]) =>
        Schema.fromIdent(t).columns.apply(b => body(b))
      }
  }
  case class curryColumnsByName[T0 <: Tuple, A](
      columnName: String,
      a: TableExpr[ReturnValueTuple[T0]]
  ) {
    inline def apply[R](
        inline body: (Schema[A *: EmptyTuple]) => TableExpr[R]
    ): TableExpr[R] =
      a.in { (t: Ident[ReturnValueTuple[T0]]) =>
        body(
          Schema.fromDelayedIdent[A](
            Expr.DelayedIdent[A](ra3.lang.Delayed(t.key, Left(columnName)))
          )
        )
      }
  }
  extension [T0 <: Tuple](a: TableExpr[ra3.lang.ReturnValueTuple[T0]]) {

    inline def columnsTuple = curryColumnsTuple[T0](a)

    inline def byName[A: NotNothing](
        n1: String
    ) =
      curryColumnsByName[T0, A](n1, a)

  }

  import scala.language.implicitConversions
  implicit def convertJoinBuilder[J, K, R <: ReturnValue[K]](
      j: ra3.lang.JoinBuilderSyntax[J, K, R]
  ): TableExpr[R] =
    j.done

  def const[T1 <: Tuple](table: ra3.Table) =
    TableExpr.Const[ReturnValueTuple[T1]](table)

  case class Const[T](table: ra3.Table) extends TableExpr[T] {

    def evalWith(env: Map[Key, TableValue])(implicit
        tsc: TaskSystemComponents
    ) =
      IO.pure(TableValue(table))

    def tags: Set[KeyTag] = Set.empty

  }
  case class Ident[+T]( key: Key) extends TableExpr[T] { self =>

     def evalWith(env: Map[Key, TableValue])(implicit
        tsc: TaskSystemComponents
    ) =
      IO.pure(env(key))

     def tags: Set[KeyTag] = key match {
      case TagKey(s) => Set(s)
      case _         => Set.empty
    }

  }

  case class Local[A, B](name: Key, assigned: TableExpr[A], body: TableExpr[B])
      extends TableExpr[B] {
    self =>

    val tags: Set[KeyTag] = name match {
      case TagKey(s) => Set(s) ++ assigned.tags ++ body.tags
      case _         => assigned.tags ++ body.tags
    }

    // def replace(i: Map[KeyTag, Int]): TableExpr = {
    //   val replacedName = name match {
    //     case TagKey(t) => IntKey(i(t))
    //     case n         => n
    //   }
    //   Local(replacedName, assigned.replace(i), body.replace(i))
    // }
    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      assigned.evalWith(env).flatMap { assignedValue =>
        body.evalWith(env + (name -> assignedValue))
      }
    }

  }

  case class SimpleQuery[I, K, A <: ReturnValue[K]](
      arg0: TableExpr.Ident[I],
      elementwise: ra3.lang.Expr[A]
  ) extends TableExpr[A] {

    val tags: Set[KeyTag] = arg0.tags

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
              Expr.toRuntime(
                elementwise,
                DelayedTableSchema(
                  Map(arg0.key -> (table.uniqueId, table.colNames))
                )
              )
            )
            .map(t => TableValue(t))

        }

    }
  }
  case class SimpleQueryCount[I, K, A <: ReturnValue[K]](
      arg0: TableExpr.Ident[I],
      elementwise: ra3.lang.Expr[A]
  ) extends TableExpr[A] {

    val tags: Set[KeyTag] = arg0.tags

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
              Expr.toRuntime(
                elementwise,
                DelayedTableSchema(
                  Map(arg0.key -> (table.uniqueId, table.colNames))
                )
              )
            )
            .map(t => TableValue(t))

        }

    }
  }
  case class Tap[T](
      val arg0: TableExpr.Ident[T],
      sampleSize: Int,
      tag: String
  ) extends TableExpr[T] {

    val tags: Set[KeyTag] = arg0.tags

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          table.showSample(sampleSize).map { str =>
            scribe.info(
              f"Tapping $tag: \n $table \n $str"
            )
            TableValue(table)
          }

        }

    }
  }
  case class GroupThenReduce[K, A <: ReturnValue[K]](
      arg0: ra3.lang.Expr.DelayedIdent[?],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[?])],
      groupwise: ra3.lang.Expr[A],
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int
  ) extends TableExpr[A] {

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)

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
              val program = Expr.toRuntime(
                groupwise,
                DelayedTableSchema(
                  Map(
                    arg0.name.table -> (
                      groupedTable.uniqueId,
                      groupedTable.colNames
                    )
                  )
                )
              )
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class GroupThenCount[K, A <: ReturnValue[K]](
      arg0: ra3.lang.Expr.DelayedIdent[?],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[?])],
      groupwise: ra3.lang.Expr[A],
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int
  ) extends TableExpr[ra3.DI64] {

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)

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
              val program = Expr.toRuntime(
                groupwise,
                DelayedTableSchema(
                  Map(
                    arg0.name.table -> (
                      groupedTable.uniqueId,
                      groupedTable.colNames
                    )
                  )
                )
              )

              GroupedTable.countGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class GroupPartialThenReduce[K, A <: ReturnValue[K]](
      arg0: ra3.lang.Expr.DelayedIdent[?],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[?])],
      groupwise: ra3.lang.Expr[A]
  ) extends TableExpr[A] {

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)

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
              val program = Expr.toRuntime(
                groupwise,
                DelayedTableSchema(
                  Map(
                    arg0.name.table -> (
                      groupedTable.uniqueId,
                      groupedTable.colNames
                    )
                  )
                )
              )
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class FullTablePartialReduce[I, K, A <: ReturnValue[K]](
      arg0: TableExpr.Ident[I],
      groupwise: ra3.lang.Expr[A]
  ) extends TableExpr[A] {

    val tags: Set[KeyTag] = arg0.tags

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

              val program = Expr.toRuntime(
                groupwise,
                DelayedTableSchema(
                  Map(
                    arg0.key -> (groupedTable.uniqueId, groupedTable.colNames)
                  )
                )
              )
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class ReduceTable[I, K, A <: ReturnValue[K]](
      arg0: TableExpr.Ident[I],
      groupwise: ra3.lang.Expr[A]
  ) extends TableExpr[A] {

    val tags: Set[KeyTag] = arg0.tags

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
              val program = Expr.toRuntime(
                groupwise,
                DelayedTableSchema(
                  Map(
                    arg0.key -> (groupedTable.uniqueId, groupedTable.colNames)
                  )
                )
              )
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class Join[J, K, R <: ReturnValue[K]](
      arg0: ra3.lang.Expr.DelayedIdent[J],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[J], String, ra3.tablelang.Key)],
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int,
      elementwise: ra3.lang.Expr[R]
  ) extends TableExpr[R] {

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_._1.tableIdent.tags)

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0.tableIdent
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          IO.parSequenceN(32)(arg1.map { case (a, b, c) =>
            a.tableIdent.evalWith(env).map(x => (x.v, a, b, c))
          }).flatMap { arg1Tables =>
            val program = Expr.toRuntime(
              elementwise,
              DelayedTableSchema(
                Map(
                  arg0.name.table -> ((table.uniqueId, table.colNames))
                ) ++ (arg1Tables.map(_._1) zip arg1.map(_._1)).map {
                  case (table, argxDelayed) =>
                    (argxDelayed.name.table, (table.uniqueId, table.colNames))
                }
              )
            )

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

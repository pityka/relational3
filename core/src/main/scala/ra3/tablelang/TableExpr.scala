package ra3.tablelang
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.GroupedTable
import ra3.lang.Expr
import ra3.NotNothing
import ra3.lang.Expr.DelayedIdent
import ra3.lang.ReturnValueTuple
import tasks.fileservice.SharedFile
import ra3.CSVColumnDefinition
import ra3.CompressionFormat
import ra3.CharacterDecoder
import scala.NamedTuple.NamedTuple

sealed trait TableExpr[+T] { self =>

  def render: String = Render.render(self, 0, Vector.empty)

  def evaluate(implicit
      tsc: TaskSystemComponents
  ): IO[ra3.Table] = evalWith(Map.empty).map(_.v)

  protected def evalWith(env: Map[Key, TableValue])(implicit
      tsc: TaskSystemComponents
  ): IO[TableValue]

  // utilities for analysis of the tree
  protected def tags: Set[KeyTag]

  protected def assignIdent[R](
      body: TableExpr.Ident[T] => TableExpr[R]
  ): TableExpr[R] = {
    val n = ra3.tablelang.TagKey(new ra3.tablelang.KeyTag)
    val b = body(TableExpr.Ident(n))
    TableExpr.Local(n, self, b)
  }

  def in[R](
      body: TableExpr[T] => TableExpr[R]
  ): TableExpr[R] = assignIdent(body)

}
object TableExpr {

  inline def fromSchema[N <: Tuple, V <: Tuple](
      tup: scala.NamedTuple.Map[NamedTuple[
        N,
        V
      ], ra3.lang.Expr.DelayedIdent]
  ): TableExpr[NamedTuple[N, V]] = {
    inline tup.toTuple match {
      case _: EmptyTuple => ???
      case tt: (ra3.lang.Expr.DelayedIdent[?] *: _) =>
        val (h *: t) = tt

        // TODO fail compile if false
        assert(
          tt.toList
            .map(_.asInstanceOf[ra3.lang.Expr.DelayedIdent[?]].name.table)
            .distinct
            .size == 1
        )
        TableExpr.Ident[NamedTuple[N, V]](h.name.table)

    }
  }

  extension [T0](a: TableExpr[T0]) {

    def tap(tag: String, size: Int = 50): TableExpr[T0] = Tap(a, size, tag)

  }

  type Elements[T0 <: Tuple] <: Tuple = T0 match {
    case EmptyTuple                        => EmptyTuple
    case Either[ra3.BufferInt, ?] *: t     => Int *: Elements[t]
    case Either[ra3.BufferLong, ?] *: t    => Long *: Elements[t]
    case Either[ra3.BufferDouble, ?] *: t  => Double *: Elements[t]
    case Either[ra3.BufferString, ?] *: t  => CharSequence *: Elements[t]
    case Either[ra3.BufferInstant, ?] *: t => Long *: Elements[t]
  }

  extension [N <: Tuple, V <: Tuple](a: TableExpr[NamedTuple[N, V]]) {

    def rename[N2 <: Tuple](implicit ev: Tuple.Size[N2] =:= Tuple.Size[V]) =
      a.asInstanceOf[TableExpr[NamedTuple[N2, V]]]

    inline def evaluateToStreamOfSingleColumn(implicit
        tsc: TaskSystemComponents
    ) = {
      import cats.effect.unsafe.implicits.global

      a.evaluate.map(_.streamOfSingleColumnChunk[V])
    }

    def concat(b: TableExpr[NamedTuple[N, V]]*): TableExpr[NamedTuple[N, V]] =
      Concat[NamedTuple[N, V]](a, b, 32)
  }

  type SchemaT[N <: Tuple, V <: Tuple] = scala.NamedTuple.Map[NamedTuple[
    N,
    V
  ], ra3.lang.Expr.DelayedIdent]

  extension [N <: Tuple, V <: Tuple](inline a: TableExpr[NamedTuple[N, V]]) {

    transparent inline def evaluateToStream(implicit
        tsc: TaskSystemComponents
    ) = {
      import cats.effect.unsafe.implicits.global

      a.evaluate.map(_.streamOfTuplesFromColumnChunks[N, V])
    }

    inline def flatMap[R](
        inline body: SchemaT[N, V] => TableExpr[R]
    ): TableExpr[R] = schema(body)

    inline def map[NR <: Tuple, VR <: Tuple](
        // practically this is the identity
        inline body: SchemaT[N, V] => SchemaT[NR, VR]
    ): TableExpr[NamedTuple[NR, VR]] = schema2[NR, VR](body)

    private inline def schema2[NR <: Tuple, VR <: Tuple](
        inline body: SchemaT[N, V] => SchemaT[NR, VR]
    ): TableExpr[NamedTuple[NR, VR]] = inline a match {
      case b: Ident[NamedTuple[N, V]] =>
        val sch = Schema.fromIdent(b)
        sch.columns((tup) => TableExpr.fromSchema[NR, VR](body(tup)))
      case _ =>
        a.assignIdent { (t: Ident[NamedTuple[N, V]]) =>
          val sch = Schema.fromIdent(t)

          sch.columns((tup) => TableExpr.fromSchema[NR, VR](body(tup)))
        }
    }
    inline def schema[R](
        inline body: SchemaT[N, V] => TableExpr[R]
    ): TableExpr[R] = inline a match {
      case b: Ident[NamedTuple[N, V]] =>
        val sch = Schema.fromIdent(b)
        sch.columns((tup) => body(tup))
      case _ =>
        a.assignIdent { (t: Ident[NamedTuple[N, V]]) =>
          val sch = Schema.fromIdent(t)

          sch.columns((tup) => body(tup))
        }
    }

  }

  def const[N <: Tuple, V <: Tuple](
      table: ra3.Table
  ): TableExpr.Const[NamedTuple[N, V]] =
    TableExpr.Const[NamedTuple[N, V]](table)

  case class Const[T](table: ra3.Table) extends TableExpr[T] {

    protected def evalWith(env: Map[Key, TableValue])(implicit
        tsc: TaskSystemComponents
    ) =
      IO.pure(TableValue(table))

    val tags: Set[KeyTag] = Set.empty

  }
  case class Ident[+T](key: Key) extends TableExpr[T] { self =>

    def evalWith(env: Map[Key, TableValue])(implicit
        tsc: TaskSystemComponents
    ) =
      IO.pure(env(key))

    val tags: Set[KeyTag] = key match {
      case TagKey(s) => Set(s)
      case _         => Set.empty
    }

  }

  case class Local[+A, B](
      name: TagKey,
      assigned: TableExpr[A],
      body: TableExpr[B]
  ) extends TableExpr[B] {
    self =>

    protected val tags: Set[KeyTag] = name match {
      case TagKey(s) => Set(s) ++ assigned.tags ++ body.tags
    }
    protected def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      assigned.evalWith(env).flatMap { assignedValue =>
        body.evalWith(env + (name -> assignedValue))
      }
    }

  }

  case class SimpleQuery[
      I,
      N <: Tuple,
      V <: Tuple,
      A <: ReturnValueTuple[N, V]
  ](
      arg0: TableExpr.Ident[I],
      elementwise: ra3.lang.Expr[A]
  ) extends TableExpr[NamedTuple[N, V]] {

    def where(i: Expr[ra3.I32Var]) = copy(elementwise = elementwise.where(i))

    protected val tags: Set[KeyTag] = arg0.tags

    protected def evalWith(
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

  case class ImportCsv[N <: Tuple, K <: Tuple](
      arg0: SharedFile,
      name: String,
      columns: Seq[CSVColumnDefinition],
      maxSegmentLength: Int,
      files: Seq[SharedFile] = Nil,
      compression: Option[CompressionFormat] = None,
      recordSeparator: String = "\r\n",
      fieldSeparator: Char = ',',
      header: Boolean = false,
      maxLines: Long = Long.MaxValue,
      bufferSize: Int = 8292,
      characterDecoder: CharacterDecoder =
        CharacterDecoder.ASCII(silent = true),
      parallelism: Int = 32
  ) extends TableExpr[
        ra3.CSVType[N, K]
      ] {

    protected val tags: Set[KeyTag] =
      Set.empty

    protected def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      ra3
        .importCsvUntyped(
          file = arg0,
          name,
          columns,
          maxSegmentLength,
          files,
          compression,
          recordSeparator,
          fieldSeparator,
          header,
          maxLines,
          bufferSize,
          characterDecoder,
          parallelism
        )
        .map { table => TableValue(table) }
    }
  }

  case class TopK[K](
      arg0: ra3.lang.Expr.DelayedIdent[?],
      ascending: Boolean,
      k: Int,
      cdfCoverage: Double,
      cdfNumberOfSamplesPerSegment: Int
  ) extends TableExpr[K] {

    protected val tags: Set[KeyTag] =
      arg0.tableIdent.tags

    protected def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0.tableIdent
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          val columnIdx =
            arg0.name.selection.left
              .map(s => table.colNames.indexOf(s))
              .fold(identity, identity)

          table
            .topK(
              sortColumn = columnIdx,
              ascending = ascending,
              k = k,
              cdfCoverage = cdfCoverage,
              cdfNumberOfSamplesPerSegment = cdfNumberOfSamplesPerSegment
            )
            .map(TableValue(_))
        }
    }
  }
  case class Prepartition[K](
      arg0: ra3.lang.Expr.DelayedIdent[?],
      arg1: Seq[ra3.lang.Expr.DelayedIdent[?]],
      partitionBase: Int,
      partitionLimit: Int,
      maxItemsToBufferAtOnce: Int
  ) extends TableExpr[K] {

    assert(arg1.forall(_.name.table == arg0.name.table))

    protected val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)

    protected def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0.tableIdent
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          val columnIdx = (arg0 +: arg1).map(
            _.name.selection.left
              .map(s => table.colNames.indexOf(s))
              .fold(identity, identity)
          )

          table
            .prePartition(
              columnIdx = columnIdx,
              partitionBase = partitionBase,
              partitionLimit = partitionLimit,
              maxItemsToBufferAtOnce = maxItemsToBufferAtOnce
            )
            .map(TableValue(_))
        }
    }
  }
  case class Concat[K](
      arg0: TableExpr[K],
      args: Seq[TableExpr[K]],
      parallelism: Int
  ) extends TableExpr[K] {

    protected val tags: Set[KeyTag] =
      arg0.tags ++ args.map(_.tags).foldLeft(Set.empty[KeyTag])(_ ++ _)

    protected def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      IO.both(
        arg0.evalWith(env),
        IO.parSequenceN(parallelism)(args.map(_.evalWith(env)))
      ).flatMap { case (TableValue(a), bs) =>
        scribe.info(s"Concatenate ${1 + bs.size} tables.")
        a.concatenate(bs.map(_.v)*).map(TableValue(_))
      }
    }
  }
  case class SimpleQueryCount[I, N <: Tuple, K <: Tuple, A <: ReturnValueTuple[
    N,
    K
  ]](
      arg0: TableExpr.Ident[I],
      elementwise: ra3.lang.Expr[A]
  ) extends TableExpr[
        NamedTuple[("count" *: EmptyTuple), ra3.DI64 *: EmptyTuple]
      ] {

    def where(i: Expr[ra3.I32Var]) = copy(elementwise = elementwise.where(i))

    protected val tags: Set[KeyTag] = arg0.tags

    protected def evalWith(
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
      val arg0: TableExpr[T],
      sampleSize: Int,
      tag: String
  ) extends TableExpr[T] {

    val tags: Set[KeyTag] = arg0.tags

    protected def evalWith(
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
  case class GroupThenReduce[N <: Tuple, K <: Tuple, A <: ReturnValueTuple[
    N,
    K
  ]](
      arg0: ra3.lang.Expr.DelayedIdent[?],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[?])],
      groupwise: ra3.lang.Expr[A],
      partitionBase: Int,
      partitionLimit: Int,
      maxItemsToBufferAtOnce: Int
  ) extends TableExpr[NamedTuple[N, K]] {

    def where(i: Expr[ra3.I32Var]) = copy(groupwise = groupwise.where(i))

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)

    protected def evalWith(
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
              maxItemsToBufferAtOnce = maxItemsToBufferAtOnce
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
  case class GroupThenCount[
      N <: Tuple,
      K <: Tuple,
      A <: ReturnValueTuple[N, K]
  ](
      arg0: ra3.lang.Expr.DelayedIdent[?],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[?])],
      groupwise: ra3.lang.Expr[A],
      partitionBase: Int,
      partitionLimit: Int,
      maxItemsToBufferAtOnce: Int
  ) extends TableExpr[
        NamedTuple["count" *: EmptyTuple, ra3.DI64 *: EmptyTuple]
      ] {

    def where(i: Expr[ra3.I32Var]) = copy(groupwise = groupwise.where(i))

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)

    protected def evalWith(
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
              maxItemsToBufferAtOnce = maxItemsToBufferAtOnce
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
  case class GroupPartialThenReduce[
      N <: Tuple,
      K <: Tuple,
      A <: ReturnValueTuple[N, K]
  ](
      arg0: ra3.lang.Expr.DelayedIdent[?],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[?])],
      groupwise: ra3.lang.Expr[A]
  ) extends TableExpr[NamedTuple[N, K]] {

    def where(i: Expr[ra3.I32Var]) = copy(groupwise = groupwise.where(i))

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)

    protected def evalWith(
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
  case class FullTablePartialReduce[
      I,
      N <: Tuple,
      K <: Tuple,
      A <: ReturnValueTuple[N, K]
  ](
      arg0: TableExpr.Ident[I],
      groupwise: ra3.lang.Expr[A]
  ) extends TableExpr[NamedTuple[N, K]] {

    def where(i: Expr[ra3.I32Var]) = copy(groupwise = groupwise.where(i))

    val tags: Set[KeyTag] = arg0.tags

    protected def evalWith(
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
  case class ReduceTable[
      I,
      N <: Tuple,
      K <: Tuple,
      A <: ReturnValueTuple[N, K]
  ](
      arg0: TableExpr.Ident[I],
      groupwise: ra3.lang.Expr[A]
  ) extends TableExpr[NamedTuple[N, K]] {

    def where(i: Expr[ra3.I32Var]) = copy(groupwise = groupwise.where(i))

    val tags: Set[KeyTag] = arg0.tags

    protected def evalWith(
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
  case class Join[J, N <: Tuple, K <: Tuple, R <: ReturnValueTuple[N, K]](
      arg0: ra3.lang.Expr.DelayedIdent[J],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[J], String, ra3.tablelang.Key)],
      partitionBase: Int,
      partitionLimit: Int,
      maxItemsToBufferAtOnce: Int,
      elementwise: ra3.lang.Expr[R]
  ) extends TableExpr[NamedTuple[N, K]] {

    def where(i: Expr[ra3.I32Var]) = copy(elementwise = elementwise.where(i))
    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_._1.tableIdent.tags)

    protected def evalWith(
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
                maxItemsToBufferAtOnce,
                program
              )
              .map(TableValue(_))
          }

        }

    }
  }

}

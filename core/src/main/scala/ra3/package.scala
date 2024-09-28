import tasks.*
import ra3.lang.*
import ra3.tablelang.*

/** ra3 provides an embedded query language and its corresponding query engine.
  *
  * ra3 is built on a distributed task execution library named tasks.
  * Consequently almost all interactions with ra3 needs a handle for a
  * configured tasks runtime environment represented by a value of the type
  * tasks.TaskSystemComponents. You can configure and start the tasks
  * environment with tasks.withTaskSystem method.
  *
  * ra3 can query data on disk (or in object storage) organized into its own
  * intermediate chunked columnar storage. A table in ra3 is represented by a
  * value of the type [[ra3.Table]]. One can import CSV data with the
  * [[ra3.importCsv]] method. One can export data back to CSV with the
  * [[ra3.Table.exportToCsv]] method. The intermediate data organization is not
  * meant for any use outside of ra3, neither for long term storage.
  *
  * Each query in ra3 is persisted to secondary storage and checkpointed.
  *
  * The entry points to the query language are the various methods in the
  * [[ra3]] package or in the [[ra3.Table]] class which provide typed references
  * to columns or references to tables, e.g.:
  *   - [[ra3.let]] and [[ra3.let0]]
  *   - [[ra3.Table.in]] and [[ra3.Table.in0]]
  *
  * The query language builds an expression tree of type
  * [[ra3.tablelang.TableExpr]], which is evaluated with the
  * [[ra3.tablelang.TableExpr.evaluate]] into an IO[Table].
  * [[ra3.tablelang.TableExpr]] is a serializable description the query. The
  * expression tree of the query may be printed in human readable form with
  * [[ra3.tablelang.TableExpr.render]].
  *
  * The following table level operators are available in ra3:
  *   - simple query, i.e. element-wise filter and projection. Corresponds to
  *     SQL queries of SELECT and WHERE.
  *   - count query, i.e. element-wise filter and count. Corresponds to SQL
  *     queries of SELECT count(*) and WHERE (but no GROUP BY).
  *   - equi-join, i.e. join with a join condition restricted to be equality
  *     among two columns.
  *   - group by then reduce. Corresponds to SELECT aggregations, WHERE, GROUP
  *     BY clauses of SQL
  *   - group by then count. Corresponds to SELECT count(*), WHERE, GROUP BY
  *     clauses of SQL
  *   - approximate selection of the top K number of elements by a single column
  *
  * Common features of SQL which are not available:
  *   - arbitrary join condition (e.g. join by inequalities)
  *   - complete products of tables (Cartesian products)
  *   - full table sort
  *   - sub-query in filter (WHERE in (select id FROM ..))
  *
  * Partitioning. ra3 does not maintain random access indexes, but it is
  * repartitioning (sharding / bucketizing) the needed columns for a given group
  * by or join operator such that all the keys needed to complete the operation
  * are in the same partition.
  *
  * Language imports. You may choose to import everything from the ra3 package.
  * It does not contain any implicits.
  */
package object ra3 {

  /** The value which encodes a missing string. It is the string of length 1,
    * with content of the \u0001 character.
    */
  val MissingString = BufferString.MissingValue.toString
  type StrVar = DStr
  type I32Var = DI32
  type I64Var = DI64
  type F64Var = DF64
  type InstVar = DInst

  type TableVariable = ra3.tablelang.TableExpr.Ident
  type TableExpr = ra3.tablelang.TableExpr

  /** Data type of I32 columns */
  private[ra3] type DI32 = Either[BufferInt, Seq[SegmentInt]]

  /** Data type of I64 columns */
  private[ra3] type DI64 = Either[BufferLong, Seq[SegmentLong]]

  /** Data type of F64 columns */
  private[ra3] type DF64 = Either[BufferDouble,Seq[SegmentDouble]]

  /** Data type of String columns */
  private[ra3] type DStr = Either[BufferString, Seq[SegmentString]]

  /** Data type of Instant columns */
  private[ra3] type DInst = Either[BufferInstant, Seq[SegmentInstant]]

  type ColumnSpecExpr[T] = Expr[ColumnSpec[T]]

  import scala.language.implicitConversions
  implicit def conversionDI32(
      a: Expr[DI32]
  ): ColumnSpecExpr[DI32] = ???//a.unnamed

  implicit def conversionDF64(
      a: Expr[DF64]
  ): ColumnSpecExpr[DF64] = ???//a.unnamed
  implicit def conversionDI64(
      a: Expr[DI64]
  ): ColumnSpecExpr[DI64] = ??? // a.unnamed
  implicit def conversionDInst(
      a: Expr[DInst]
  ): ColumnSpecExpr[DInst] = ??? // a.unnamed

  implicit def conversionStr(
      a: Expr[DStr]
  ): ColumnSpecExpr[DStr] = ??? // a.unnamed

  /** Import CSV data into ra3
    *
    * @param file
    * @param name
    *   Name of the table to create, must be unique
    * @param columns
    *   Description of columns: at a minimum the 0-based column index in the csv
    *   file and the type of the column
    * @param maxSegmentLength
    *   Each column will be chunked to this length
    * @param compression
    * @param recordSeparator
    * @param fieldSeparator
    * @param header
    * @param maxLines
    * @param bufferSize
    * @param characterDecoder
    * @return
    */
  def importCsv(
      file: SharedFile,
      name: String,
      columns: Seq[CSVColumnDefinition],
      maxSegmentLength: Int,
      compression: Option[CompressionFormat] = None,
      recordSeparator: String = "\r\n",
      fieldSeparator: Char = ',',
      header: Boolean = false,
      maxLines: Long = Long.MaxValue,
      bufferSize: Int = 8292,
      characterDecoder: CharacterDecoder = CharacterDecoder.ASCII(silent = true)
  )(implicit
      tsc: TaskSystemComponents
  ) = ra3.ts.ImportCsv.queue(
    file,
    name,
    columns.map {
      case CSVColumnDefinition.I32Column(columnIndex) =>
        (columnIndex, ColumnTag.I32, None, None)
      case CSVColumnDefinition.I64Column(columnIndex) =>
        (columnIndex, ColumnTag.I64, None, None)
      case CSVColumnDefinition.F64Column(columnIndex) =>
        (columnIndex, ColumnTag.F64, None, None)
      case CSVColumnDefinition.StrColumn(columnIndex, missingValue) =>
        (columnIndex, ColumnTag.StringTag, None, missingValue)
      case CSVColumnDefinition.InstantColumn(columnIndex, parser) =>
        (columnIndex, ColumnTag.Instant, parser, None)
    },
    recordSeparator,
    fieldSeparator,
    header,
    maxLines,
    maxSegmentLength,
    compression,
    bufferSize,
    characterDecoder
  )

  //

  /** The expression which selects all column as is */
  // val star: Expr { type T = ra3.lang.ColumnSpec[Any] } = ra3.lang.Expr.Star

  /** Variable assigning let expression.
    *
    * @param assigned
    *   the expression which is being assigned
    * @param body
    *   the variable (the scala variable which is bound in the lambda) and the
    *   expression which is using the variable
    */
  def let[T1,R](assigned: Expr[T1])(body: Expr[T1] => Expr[R]): Expr[R] =
    ra3.lang.local(assigned)(body)

  /** Elementwise or group wise projection */
  def select[T0](
      arg0: ColumnSpecExpr[T0]
  ): Expr[ra3.lang.ReturnValue1[T0]] = ra3.lang.Expr
    .BuiltInOp1(new ops.Op1.MkReturnValue1[T0])(arg0 )

  def select[T0, T1](
      arg0: ColumnSpecExpr[T0],
      arg1: ColumnSpecExpr[T1]
  ) = ra3.lang.Expr
    .BuiltInOp2(new ops.Op2.MkReturnValue2[T0,T1])(arg0, arg1 )
  def select[T0, T1, T2](
      arg0: ColumnSpecExpr[T0],
      arg1: ColumnSpecExpr[T1],
      arg2: ColumnSpecExpr[T2],
  ) = ra3.lang.Expr
    .BuiltInOp3(new ops.Op3.MkReturnValue3[T0,T1,T2])(arg0, arg1,arg2 )
  def select[T0, T1, T2,T3](
      arg0: ColumnSpecExpr[T0],
      arg1: ColumnSpecExpr[T1],
      arg2: ColumnSpecExpr[T2],
      arg3: ColumnSpecExpr[T3]
  ) = ra3.lang.Expr
    .BuiltInOp4(new ops.OpN.MkReturnValue4[T0,T1,T2,T3])(arg0, arg1,arg2, arg3 )
  def select[T0, T1, T2,T3,T4](
      arg0: ColumnSpecExpr[T0],
      arg1: ColumnSpecExpr[T1],
      arg2: ColumnSpecExpr[T2],
      arg3: ColumnSpecExpr[T3],
      arg4: ColumnSpecExpr[T4]
  ) = ra3.lang.Expr
    .BuiltInOp5(new ops.OpN.MkReturnValue5[T0,T1,T2,T3,T4])(arg0, arg1,arg2, arg3,arg4 )

  //  inline def selectTuple[T0<:Tuple:Tuple.IsMappedBy[ColumnSpecExpr]](
  //   tup: T0
  // )  = 
  //   ra3.lang.Expr.BuiltInOpAny(tup.productIterator.map(_.asInstanceOf[Expr]).toList,ops.OpAny.MkReturnValueStar).asInstanceOf[Expr { 

  //     type T = ReturnValueTuple[Tuple.InverseMap[T0,ColumnSpecExpr]] 
  //   }]
  

  // def where(arg0: I32ColumnExpr) =
  //   Expr.makeOp1(ops.Op1.MkRawWhere)(arg0)
  // def filter(arg0: I32ColumnExpr): ReturnExpr =
  //   where(arg0)

  /** Simple query consisting of elementwise (row-wise) projection and filter */
  def query[T](prg: ra3.lang.Query[T]) = {
    val tables = prg.referredTables
    require(
      tables.size == 1,
      s"0 or more than 1 tables referenced in this query $prg"
    )
    ra3.tablelang.TableExpr.SimpleQuery(tables.head, prg)
  }

  /** Count query consisting of elementwise (row-wise) filter and counting those
    * rows which pass the filter
    */

  def count[T](prg: ra3.lang.Query[T]) = {
    val tables = prg.referredTables
    require(
      tables.size == 1,
      s"0 or more than 1 tables referenced in this query $prg"
    )
    ra3.tablelang.TableExpr.SimpleQueryCount(tables.head, prg)
  }

  /** Full table reduction
    *
    * Equivalent to a group by into a single group, then reducing that single
    * group. Returns a single row.
    *
    * This will read all rows of the needed columns into memory. You may want to
    * consult with partialReduce if the reduction is distributable.
    */
  def reduce[T](
      prg: ra3.lang.Query[T]
  ) = {
    val tables = prg.referredTables

    require(
      tables.size == 1,
      s"0 or more than 1 tables referenced in this query $prg"
    )
    ra3.tablelang.TableExpr.ReduceTable(
      arg0 = tables.head,
      groupwise = prg
    )
  }

  /** Partial reduction
    *
    * Reduces each segment independently. Returns a single row per segment.
    */
  def partialReduce[T](
      prg: ra3.lang.Query[T]
  ) = {
    val tables = prg.referredTables

    require(
      tables.size == 1,
      s"0 or more than 1 tables referenced in this query $prg"
    )
    ra3.tablelang.TableExpr.FullTablePartialReduce(
      arg0 = tables.head,
      groupwise = prg
    )
  }


 

  /** Concatenate the list of rows of multiple tables ('grows downwards') */
  def concatenate(
      others: Table*
  )(implicit tsc: TaskSystemComponents): cats.effect.IO[Table] = {
    val name = ra3.ts.MakeUniqueId.queue(
      others.head,
      s"concat",
      others.flatMap(_.columns).map(_.column)
    )
    name.map { name =>
      val all = others
      assert(all.map(_.colNames).distinct.size == 1)
      assert(all.map(_.columns.map(_.tag)).distinct.size == 1)
      val columns = all.head.columns.size
      val cols = 0 until columns map { cIdx =>
        all.map(_.columns(cIdx)).reduce(_ `castAndConcatenate` _)
      } toVector

      Table(cols, all.head.colNames, name, None)
    }
  }
  def render(q: TableExpr) = ">>\n" + Render.render(q.replaceTags(), 0) + "\n<<"
}

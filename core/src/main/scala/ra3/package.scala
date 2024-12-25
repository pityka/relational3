import tasks.*
import ra3.lang.*
import ra3.tablelang.*
import cats.effect.IO
import scala.reflect.ClassTag

/** ra3 provides an embedded query language and its corresponding query engine.
  *
  * ra3 is built on a distributed task execution library named tasks.
  * Consequently almost all interactions with ra3 need a handle for a configured
  * runtime environment represented by a value of the type
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
  * [[ra3]] package or in the [[ra3.tablelang.TableExpr]] class which provide
  * typed references to columns or references to tables, e.g.:
  *   - [[ra3.tablelang.TableExpr.scheam]]
  *
  * The query language builds an expression tree of type
  * [[ra3.tablelang.TableExpr]], which is evaluated with the
  * [[ra3.tablelang.TableExpr.evaluate]] into an IO[Table].
  * [[ra3.tablelang.TableExpr]] is a description the query. The expression tree
  * of the query may be printed in human readable form with
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
  * repartitioning (sharding / bucketizing / shuffling) the needed columns for a
  * given group by or join operator such that all the keys needed to complete
  * the operation are in the same partition.
  *
  * Language imports. You may choose to import everything from the ra3 package.
  * It does not contain any implicits.
  */
package object ra3 {

  // type Elem[T] = T match {
  //   case ra3.BufferInt     => Int
  //   case ra3.BufferLong    => Long
  //   case ra3.BufferDouble  => Double
  //   case ra3.BufferString  => CharSequence
  //   case ra3.BufferInstant => Long
  // }
  type M2[T <: Tuple] <: Tuple = T match {
    case EmptyTuple   => EmptyTuple
    case StrVar *: t  => CharSequence *: M2[t]
    case I32Var *: t  => Int *: M2[t]
    case I64Var *: t  => Long *: M2[t]
    case F64Var *: t  => Double *: M2[t]
    case InstVar *: t => Long *: M2[t]

  }

  /** The value which encodes a missing string. It is the string of length 1,
    * with content of the \u0001 character.
    */
  val MissingString = BufferString.MissingValue.toString
  sealed trait Box[+T]
  case class StrVar(v: DStr) extends Box[DStr]
  case class I32Var(v: DI32) extends Box[DI32]
  case class I64Var(v: DI64) extends Box[DI64]
  case class F64Var(v: DF64) extends Box[DF64]
  case class InstVar(v: DInst) extends Box[DInst]

  /** Table query with value type R
    */
  type TQ[R] = ra3.tablelang.TableExpr[R]

  /** Row query with valeu type A
    */
  type RQ[A] = ra3.lang.Expr[A]

  /** Data type of I32 columns */
  private[ra3] type DI32 = Either[BufferInt, Seq[SegmentInt]]

  /** Data type of I64 columns */
  private[ra3] type DI64 = Either[BufferLong, Seq[SegmentLong]]

  /** Data type of F64 columns */
  private[ra3] type DF64 = Either[BufferDouble, Seq[SegmentDouble]]

  /** Data type of String columns */
  private[ra3] type DStr = Either[BufferString, Seq[SegmentString]]

  /** Data type of Instant columns */
  private[ra3] type DInst = Either[BufferInstant, Seq[SegmentInstant]]

  private[ra3] type Primitives =
    DI32 | DStr | DInst | DF64 | DI64 | String | Int | Long | Double | String |
      java.time.Instant
  type ColumnSpecExpr[N <: String, T <: Primitives] = Expr[ColumnSpec[N, T]]

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
  def importCsvUntyped(
      file: SharedFile,
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
  )(implicit
      tsc: TaskSystemComponents
  ) = {

    def do1(f: SharedFile) = ra3.ts.ImportCsv.queue(
      f,
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

    val f1 = do1(file)
    val fs = files.map(do1)
    IO.both(f1, IO.parSequenceN(parallelism)(fs)).flatMap { case (t1, ts) =>
      t1.concatenate(ts*)
    }
  }

  type CSVType[N <: Tuple, T <: Tuple] =
    scala.NamedTuple.NamedTuple[N, Tuple.Map[T, CsvColumnDefToColumnType]]

  type CsvColumnDefToColumnType[T] = T match {
    case CSVColumnDefinition.I32Column     => ra3.I32Var
    case CSVColumnDefinition.I64Column     => ra3.I64Var
    case CSVColumnDefinition.F64Column     => ra3.F64Var
    case CSVColumnDefinition.StrColumn     => ra3.StrVar
    case CSVColumnDefinition.InstantColumn => ra3.InstVar
  }
  private inline def untuple[T <: Tuple](t: T): List[CSVColumnDefinition] =
    inline t match {
      case EmptyTuple => Nil
      case ht: (h *: t) =>
        val h *: t = ht
        inline h match {
          case h2: CSVColumnDefinition.I32Column     => h2 :: untuple(t)
          case h2: CSVColumnDefinition.I64Column     => h2 :: untuple(t)
          case h2: CSVColumnDefinition.F64Column     => h2 :: untuple(t)
          case h2: CSVColumnDefinition.StrColumn     => h2 :: untuple(t)
          case h2: CSVColumnDefinition.InstantColumn => h2 :: untuple(t)
        }
    }

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
  inline def importCsv[N <: Tuple, T <: Tuple](
      file: SharedFile,
      name: String,
      columns: NamedTuple.NamedTuple[N, T],
      maxSegmentLength: Int,
      files: Seq[SharedFile] = Nil,
      compression: Option[CompressionFormat] = None,
      recordSeparator: String = "\r\n",
      fieldSeparator: Char = ',',
      header: Boolean = false,
      maxLines: Long = Long.MaxValue,
      bufferSize: Int = 8292,
      characterDecoder: CharacterDecoder = CharacterDecoder.ASCII(silent = true)
  ): TableExpr[CSVType[N, T]] = {
    val list = untuple(columns)

    ra3.tablelang.TableExpr.ImportCsv(
      file,
      name,
      list,
      maxSegmentLength,
      files,
      compression,
      recordSeparator,
      fieldSeparator,
      header,
      maxLines,
      bufferSize,
      characterDecoder
    )
  }

  inline def importFromStream[N <: Tuple, T <: Tuple: ClassTag](
      stream: fs2.Stream[IO, scala.NamedTuple.NamedTuple[N, T]],
      uniqueId: String,
      minimumSegmentSize: Int,
      maximumSegmentSize: Int
  )(implicit tsc: TaskSystemComponents) =
    ImportFromStream.importFromStream[N, T](
      stream,
      uniqueId,
      minimumSegmentSize,
      maximumSegmentSize
    )

  def const(s: Long): Expr[Long] =
    ra3.lang.Expr.makeOp0(ops.Op0.ConstantI64(s))
  def const(s: Int): Expr[Int] = ra3.lang.Expr.makeOp0(ops.Op0.ConstantI32(s))
  def const(s: String): Expr[String] =
    ra3.lang.Expr.makeOp0(ops.Op0.ConstantString(s))
  def const(s: Double): Expr[Double] =
    ra3.lang.Expr.makeOp0(ops.Op0.ConstantF64(s))
  def const(s: java.time.Instant): Expr[java.time.Instant] =
    ra3.lang.Expr.makeOp0(ops.Op0.ConstantInstant(s))

  def LitI64S(s: Set[Long]): Expr[Set[Long]] =
    ra3.lang.Expr.makeOp0(ops.Op0.ConstantI64S(s))
  def LitI32S(s: Set[Int]): Expr[Set[Int]] =
    ra3.lang.Expr.makeOp0(ops.Op0.ConstantI32S(s))
  def LitStringS(s: Set[String]): Expr[Set[String]] =
    ra3.lang.Expr.makeOp0(ops.Op0.ConstantStringS(s))
  def LitF64S(s: Set[Double]): Expr[Set[Double]] =
    ra3.lang.Expr.makeOp0(ops.Op0.ConstantF64S(s))
  def LitInstS(s: Set[java.time.Instant]): Expr[Set[java.time.Instant]] =
    ra3.lang.Expr.makeOp0(ops.Op0.ConstantInstantS(s))

  /** Elementwise or group wise projection */
  def select0: Expr[ra3.lang.ReturnValueTuple[EmptyTuple, EmptyTuple]] =
    ra3.lang.Expr
      .makeOp0(ops.Op0.ConstantEmptyReturnValue)
  def S: Expr[ra3.lang.ReturnValueTuple[EmptyTuple, EmptyTuple]] = select0

  inline def all[
      N <: Tuple,
      V <: Tuple
  ](
      tuple: scala.NamedTuple.NamedTuple[N, V]
  ) = ra3.tablelang.Schema.extendAll[N,V,""](tuple)
  inline def allWithPrefix[
      N <: Tuple,
      V <: Tuple,
      P <: String & Singleton
  ](
      tuple: scala.NamedTuple.NamedTuple[N, V],
      prefix: String
  ) = ra3.tablelang.Schema.extendAll[N,V,prefix.type](tuple)

  def where(arg0: ra3.lang.util.I32ColumnExpr) =
    select0.where(arg0)
  def filter(arg0: ra3.lang.util.I32ColumnExpr) =
    where(arg0)

  /** Simple query consisting of elementwise (row-wise) projection and filter */
  def select[N <: Tuple, T <: Tuple](
      prg: ra3.lang.Expr[ra3.lang.ReturnValueTuple[N, T]]
  ) =
    query(prg)

  /** Simple query consisting of elementwise (row-wise) projection and filter */
  def query[N <: Tuple, T <: Tuple](
      prg: ra3.lang.Expr[ra3.lang.ReturnValueTuple[N, T]]
  ) = {
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

  def count[N <: Tuple, T <: Tuple](
      prg: ra3.lang.Expr[ra3.lang.ReturnValueTuple[N, T]]
  ) = {
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
  def reduce[N <: Tuple, T <: Tuple](
      prg: ra3.lang.Expr[ra3.lang.ReturnValueTuple[N, T]]
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
  def partialReduce[N <: Tuple, T <: Tuple](
      prg: ra3.lang.Expr[ra3.lang.ReturnValueTuple[N, T]]
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
  def render[T](q: TableExpr[T]) =
    ">>\n" + Render.render(q, 0, Vector.empty) + "\n<<"
}

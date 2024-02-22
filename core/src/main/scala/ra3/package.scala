import tasks._
import ra3.lang._
import ra3.tablelang._
package object ra3 {
  val MissingString = BufferString.MissingValue.toString

  type ColumnVariable[T] = ra3.lang.DelayedIdent[T]

  type TableVariable = ra3.tablelang.TableExpr.Ident

  type DI32 = Either[BufferInt, Seq[SegmentInt]]
  type DI64 = Either[BufferLong, Seq[SegmentLong]]
  type DF64 = Either[BufferDouble, Seq[SegmentDouble]]
  type DStr = Either[BufferString, Seq[SegmentString]]
  type DInst = Either[BufferInstant, Seq[SegmentInstant]]

  sealed trait CSVColumnDefinition
  case class I32Column(columnIndex: Int) extends CSVColumnDefinition
  case class I64Column(columnIndex: Int) extends CSVColumnDefinition
  case class F64Column(columnIndex: Int) extends CSVColumnDefinition
  case class StrColumn(columnIndex: Int, missingValue: Option[String] = None)
      extends CSVColumnDefinition
  case class InstantColumn(
      columnIndex: Int,
      parser: Option[ra3.ts.ImportCsv.InstantFormat] = None
  ) extends CSVColumnDefinition

  def importCsv(
      file: SharedFile,
      name: String,
      columns: Seq[CSVColumnDefinition],
      maxSegmentLength: Int,
      compression: Option[ra3.ts.ImportCsv.CompressionFormat] = None,
      recordSeparator: String = "\r\n",
      fieldSeparator: Char = ',',
      header: Boolean = false,
      maxLines: Long = Long.MaxValue,
      bufferSize: Int = 8292,
      characterDecoder: ra3.ts.ImportCsv.CharacterDecoder =
        ra3.ts.ImportCsv.ASCII(silent = true)
  )(implicit
      tsc: TaskSystemComponents
  ) = ra3.ts.ImportCsv.queue(
    file,
    name,
    columns.map {
      case I32Column(columnIndex) => (columnIndex, ColumnTag.I32, None, None)
      case I64Column(columnIndex) => (columnIndex, ColumnTag.I64, None, None)
      case F64Column(columnIndex) => (columnIndex, ColumnTag.F64, None, None)
      case StrColumn(columnIndex, missingValue) =>
        (columnIndex, ColumnTag.StringTag, None, missingValue)
      case InstantColumn(columnIndex, parser) =>
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

  val star: Expr { type T = ColumnSpec } = ra3.lang.Expr.Star

  def let[T1](assigned: Expr)(body: Expr { type T = assigned.T } => Expr {
    type T = T1
  }): Expr { type T = T1 } =
    ra3.lang.local(assigned)(body)

  def select(args: Expr { type T = ColumnSpec }*): ReturnExpr = {
    Expr.makeOpStar(ops.MkSelect)(args: _*)
  }
  def project(args: Expr { type T = ColumnSpec }*): ReturnExpr = {
    Expr.makeOpStar(ops.MkSelect)(args: _*)
  }

  def query(prg: ra3.lang.Query) = {
    val tables = prg.referredTables
    require(
      tables.size == 1,
      s"0 or more than 1 tables referenced in this query $prg"
    )
    ra3.tablelang.TableExpr.SimpleQuery(tables.head, prg)
  }
  def count(prg: ra3.lang.Query) = {
    val tables = prg.referredTables
    require(
      tables.size == 1,
      s"0 or more than 1 tables referenced in this query $prg"
    )
    ra3.tablelang.TableExpr.SimpleQueryCount(tables.head, prg)
  }

  def reduce(
      prg: ra3.lang.Query
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
  def partialReduce(
      prg: ra3.lang.Query
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

  // *****

  def useTable(assigned: ra3.Table)(
      body: TableExpr.Ident => TableExpr
  ): TableExpr =
    local(TableExpr.Const(assigned))(body)
  def let0(assigned: ra3.Table)(
      body: TableExpr.Ident => TableExpr
  ): TableExpr =
    local(TableExpr.Const(assigned))(body)
  def let[T0: NotNothing](assigned: ra3.Table)(
      body: (ra3.lang.DelayedIdent[T0]) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(assigned)) { t =>
      t.useColumn[T0](0) { c0 =>
        body(c0)
      }
    }
  }
  def let[T0: NotNothing, T1: NotNothing](assigned: ra3.Table)(
      body: (
          ra3.lang.DelayedIdent[T0],
          ra3.lang.DelayedIdent[T1]
      ) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(assigned)) { t =>
      t.useColumns[T0, T1](0, 1) { case (c0, c1) =>
        body(c0, c1)
      }
    }
  }
  def let[T0: NotNothing, T1: NotNothing, T2: NotNothing](
      assigned: ra3.Table
  )(
      body: (
          ra3.lang.DelayedIdent[T0],
          ra3.lang.DelayedIdent[T1],
          ra3.lang.DelayedIdent[T2]
      ) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(assigned)) { t =>
      t.useColumns[T0, T1, T2](0, 1, 2) { case (c0, c1, c2) =>
        body(c0, c1, c2)
      }
    }
  }
  def let[T0: NotNothing, T1: NotNothing, T2: NotNothing, T3: NotNothing](
      assigned: ra3.Table
  )(
      body: (
          ra3.lang.DelayedIdent[T0],
          ra3.lang.DelayedIdent[T1],
          ra3.lang.DelayedIdent[T2],
          ra3.lang.DelayedIdent[T3]
      ) => TableExpr
  ): TableExpr = {
    local(TableExpr.Const(assigned)) { t =>
      t.useColumns[T0, T1, T2, T3](0, 1, 2, 3) { case (c0, c1, c2, c3) =>
        body(c0, c1, c2, c3)
      }
    }
  }
}

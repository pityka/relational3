package ra3.tablelang
import ra3.lang.*
import ra3.lang
import ra3.lang.ops.Op3.*
import ra3.lang.ops.Op2.*
import ra3.lang.ops.Op1.*
import ra3.lang.ops.Op0.*
import ra3.lang.Expr.*
import ra3.lang.ops.OpAny
// import ra3.lang.ops.OpStar.MkSelect
private[ra3] object Render {

  def render(op: ra3.lang.ops.Op0): String = op match {
    case ConstantEmptyReturnValue => "S0"
    case op                       => s"`${op.op().toString}`"
  }
  def render(op: ra3.lang.ops.Op3): String = op match {
    case BufferCountInGroupsOpL            => "in"
    case IfElseF64                         => "ifelse"
    case BufferMinGroupsOpInst             => "min"
    case BufferFirstGroupsOpSIi            => "first"
    case IfElseI32CC                       => "ifelse"
    case BufferCountDistinctInGroupsOpS    => "countdistinct"
    case BufferHasMissingInGroupsOpS       => "hasmissing"
    case StringMatchAndReplaceOp           => "gsub"
    case BufferSumGroupsOpDI               => "sum"
    case BufferCountInGroupsOpInst         => "count"
    case IfElseI32C                        => "ifelse"
    case BufferCountDistinctInGroupsOpI    => "countdistinct"
    case BufferMaxGroupsOpI                => "max"
    case IfElseCStr                        => "ifelse"
    case BufferAllInGroupsOpI              => "all"
    case BufferCountInGroupsOpI            => "count"
    case BufferCountDistinctInGroupsOpD    => "countdistinct"
    case BufferCountGroupsOpDI             => "count"
    case IfElseI64C                        => "ifelse"
    case IfElseStrC                        => "iflese"
    case BufferHasMissingInGroupsOpInst    => "isnull"
    case IfElseStrCC                       => "ifelse"
    case BufferCountInGroupsOpS            => "count"
    case IfElseCI64                        => "ifelse"
    case IfElseF64CC                       => "ifelse"
    case IfElseI64CC                       => "ifelse"
    case IfElseCInst                       => "ifelse"
    case BufferMaxGroupsOpInst             => "max"
    case IfElseStr                         => "ifelse"
    case BufferCountDistinctInGroupsOpInst => "countdistinct"
    case BufferSumGroupsOpII               => "sum"
    case IfElseInstCC                      => "ifelse"
    case BufferHasMissingInGroupsOpD       => "hasnull"
    case BufferMaxGroupsOpD                => "max"
    case IfElseCF64                        => "ifelse"
    case BufferHasMissingInGroupsOpI       => "isnull"
    case BufferFirstGroupsOpDIi            => "first"
    case BufferCountDistinctInGroupsOpL    => "countdistinct"
    case BufferNoneInGroupsOpI             => "none"
    case BufferHasMissingInGroupsOpL       => "isnull"
    case BufferMeanGroupsOpDI              => "mean"
    case BufferFirstGroupsOpIIi            => "first"
    case BufferMinGroupsOpI                => "min"
    case BufferMinGroupsOpD                => "min"
    case IfElseF64C                        => "ifelse"
    case BufferFirstGroupsOpL              => "first"
    case BufferSubstringOpS                => "substr"
    case IfElseCI32                        => "ifelse"
    case IfElseI64                         => "ifelse"
    case BufferAnyInGroupsOpI              => "any"
    case IfElseI32                         => "ifelse"
    case IfElseInst                        => "ifelse"
    case IfElseInstC                       => "ifelse"
    case BufferFirstGroupsOpInst           => "first"
  }

  @scala.annotation.nowarn
  def render(op: ra3.lang.ops.Op2): String = op match {
    case ColumnLtOpDcD                 => "<"
    case ColumnGtEqOpDcD               => ">="
    case ColumnNEqOpDcD                => "!="
    case ColumnEqOpDcD                 => "=="
    case ColumnLtEqOpDcD               => "<"
    case ColumnGtOpDcD                 => ">"
    case ColumnContainedInOpIcISet     => "in"
    case ColumnAddOpDD                 => "+"
    case ColumnDivOpDD                 => "/"
    case ColumnMulOpDD                 => "*"
    case ColumnSubtractOpDD            => "-"
    case ColumnEqOpInstcL              => "=="
    case ColumnNEqOpInstcL             => "!="
    case ColumnGtEqOpInstcL            => ">="
    case ColumnGtOpInstcL              => ">"
    case ColumnLtOpInstcL              => "<"
    case ColumnLtEqOpInstcL            => "<="
    case ColumnEqOpLL                  => "=="
    case AddOp                         => "+"
    case MinusOp                       => "-"
    case ColumnMinusOpInstcL           => "-"
    case ColumnPlusOpInstcL            => "+"
    case ColumnMinusDaysOpInstcInt     => "-d"
    case ColumnMinusSecondsOpInstcInt  => "-s"
    case ColumnPlusDaysOpInstcInt      => "+d"
    case ColumnPlusSecondsOpInstcInt   => "+s"
    case ColumnNEqOpStrcStr            => "!="
    case ColumnLtEqOpStrcStr           => "<="
    case ColumnLtOpStrcStr             => "<"
    case ColumnEqOpStrcStr             => "=="
    case ColumnGtEqOpStrcStr           => ">="
    case ColumnGtOpStrcStr             => ">"
    case ColumnMatchesOpStrcStr        => "~"
    case _: MkReturnWhere[?]           => "WHERE"
    case ColumnEqOpLcL                 => "=="
    case ColumnNEqOpII                 => "!="
    case ColumnEqOpII                  => "=="
    case ColumnLtEqOpII                => "<="
    case ColumnLtOpII                  => "<"
    case ColumnGtOpII                  => ">"
    case ColumnGtEqOpII                => ">="
    case ColumnAndOpII                 => "and"
    case ColumnOrOpII                  => "or"
    case ColumnGtEqOpInstInst          => ">="
    case ColumnEqOpInstInst            => "=="
    case ColumnLtEqOpInstInst          => "<="
    case ColumnNEqOpInstInst           => "!="
    case ColumnGtOpInstInst            => "."
    case ColumnLtOpInstInst            => "<"
    case MkNamedConstantF64            => "as"
    case _: lang.ops.Op2.Tap[?]        => "tap"
    case ColumnPrintfOpDcStr           => "printf"
    case MkNamedColumnSpecChunkI32     => "as"
    case MkNamedColumnSpecChunkI64     => "as"
    case MkNamedColumnSpecChunkF64     => "as"
    case MkNamedColumnSpecChunkString  => "as"
    case MkNamedColumnSpecChunkInst    => "as"
    case MkNamedConstantStr            => "as"
    case MkNamedConstantI32            => "as"
    case ColumnConcatOpStrcStr         => "++"
    case ColumnContainedInOpStrcStrSet => "in"
    case ColumnNEqOpStrStr             => "!="
    case ColumnGtOpStrStr              => ">"
    case ColumnLtOpStrStr              => "<"
    case ColumnEqOpStrStr              => "=="
    case ColumnGtEqOpStrStr            => ">="
    case ColumnLtEqOpStrStr            => "<="
    case ColumnPrintfOpIcStr           => "printf"
    case MkNamedConstantI64            => "as"
    case ColumnGtEqOpDD                => ">="
    case ColumnEqOpDD                  => "=="
    case ColumnLtOpDD                  => "<"
    case ColumnLtEqOpDD                => "<="
    case ColumnGtOpDD                  => ">"
    case ColumnNEqOpDD                 => "!="
    case ColumnLtEqOpIcI               => "<"
    case ColumnGtEqOpIcI               => ">="
    case ColumnNEqOpIcI                => "!="
    case ColumnLtOpIcI                 => "<"
    case ColumnEqOpIcI                 => "=="
    case ColumnGtOpIcI                 => ">"
    case ColumnPrintfOpLcStr           => "printf"
    case ColumnContainedInOpDcDSet     => "in"
    case ColumnLtEqOpInstcStr          => "<="
    case ColumnLtOpInstcStr            => "<"
    case ColumnNEqOpInstcStr           => "!="
    case ColumnEqOpInstcStr            => "=="
    case ColumnGtOpInstcStr            => ">"
    case ColumnGtEqOpInstcStr          => ">="
    case ColumnConcatOpStrStr          => "++"
    case ExtendReturnUntyped           => ":*"
    case MkReturnWhereUntyped          => "WHERE"
  }
  def renderOp1(
      op: ra3.lang.ops.Op1,
      arg: ra3.lang.Expr[?],
      keyTags: Seq[ra3.tablelang.KeyTag]
  ): String =
    op match {
      case ColumnToInstantEpochMilliOpL =>
        render(arg, keyTags) + ".toEpochMilli"
      case MkUnnamedConstantI32         => render(arg, keyTags)
      case MkUnnamedColumnSpecChunkI32  => render(arg, keyTags)
      case MkUnnamedColumnSpecChunkI64  => render(arg, keyTags)
      case MkUnnamedColumnSpecChunkF64  => render(arg, keyTags)
      case MkUnnamedColumnSpecChunkStr  => render(arg, keyTags)
      case MkUnnamedColumnSpecChunkInst => render(arg, keyTags)
      case MkUnnamedConstantI64         => render(arg, keyTags)
      case ColumnIsMissingOpL           => render(arg, keyTags) + ".isnull"
      case ColumnIsMissingOpD           => render(arg, keyTags) + ".isnull"
      case ColumnRoundToIntOpD          => render(arg, keyTags) + ".toInt"
      case MkUnnamedConstantF64         => render(arg, keyTags)
      case ColumnToDoubleOpL            => render(arg, keyTags) + ".toDouble"
      case ColumnIsMissingOpInst        => render(arg, keyTags) + ".isnull"
      case ColumnNanosecondsOpInst      => render(arg, keyTags) + ".nanoseconds"
      case ColumnMinutesOpInst          => render(arg, keyTags) + ".minutes"
      case ColumnSecondsOpInst          => render(arg, keyTags) + ".seconds"
      case ColumnYearsOpInst            => render(arg, keyTags) + ".years"
      case ColumnHoursOpInst            => render(arg, keyTags) + ".hours"
      case ColumnMonthsOpInst           => render(arg, keyTags) + ".months"
      case ColumnDaysOpInst             => render(arg, keyTags) + ".days"
      case ToString                     => render(arg, keyTags) + ".toString"
      case MkUnnamedConstantStr         => render(arg, keyTags)
      case ColumnToISOOpInst            => render(arg, keyTags) + ".toInstISO"
      case ColumnToLongOpInst           => render(arg, keyTags) + ".toLong"
      case ColumnParseI64OpStr          => render(arg, keyTags) + ".toLong"
      case ColumnParseInstOpStr         => render(arg, keyTags) + ".toInst"
      case ColumnAbsOpI                 => render(arg, keyTags) + ".abs"
      case ColumnIsMissingOpI           => render(arg, keyTags) + ".isnull"
      case ColumnNotOpI                 => "not(" + render(arg, keyTags) + ")"
      // case MkRawWhere                 => "WHERE " + render(arg)
      case ColumnParseF64OpStr      => render(arg, keyTags) + ".toDouble"
      case ColumnToDoubleOpInst     => render(arg, keyTags) + ".toDouble"
      case ColumnIsMissingOpStr     => render(arg, keyTags) + ".isnull"
      case ColumnParseI32OpStr      => render(arg, keyTags) + ".toInt"
      case ColumnRoundToDayOpInst   => render(arg, keyTags) + ".rounddays"
      case ColumnRoundToHourOpInst  => render(arg, keyTags) + ".roundhours"
      case ColumnRoundToMonthOpInst => render(arg, keyTags) + ".roundmonths"
      case ColumnRoundToYearOpInst  => render(arg, keyTags) + ".roundyears"
      case AbsOp                    => render(arg, keyTags) + ".abs"
      case ColumnAbsOpD             => render(arg, keyTags) + ".abs"
      case ColumnRoundToDoubleOpD   => render(arg, keyTags) + ".toDouble"
      case ColumnToDoubleOpI        => render(arg, keyTags) + ".toDouble"
      case x => x.toString() + s"(${render(arg, keyTags)})"
    }
  // def render(op: ra3.lang.ops.OpStar): String = op match {
  //   case lang.ops.OpStar.MkSelect => "SELECT"

  // }

  def render(expr: Expr[?], tableTags: Seq[ra3.tablelang.KeyTag]): String =
    expr match {
      case Expr.Local(name, assigned, body) =>
        val rAssigned = assigned match {
          case _: Local[?] =>
            s"(${render(assigned, tableTags)})"
          case _ => render(assigned, tableTags)
        }
        val rLet = s"${render(Ident(name), tableTags)}"
        f"${render(body, tableTags)} WITH ($rLet = $rAssigned)"
      case e @ BuiltInOpAny(op) =>
        val args = e.args.filter {
          _ match {
            case Expr.Ident(_: SingletonKey) => false
            case _                           => true
          }
        }
        if (args.size == 1) s"${render(args.head, tableTags)}.${op.toString}"
        else
          s"${op.toString}(${args.map(v => render(v, tableTags)).mkString(", ")})"
      case e @ BuiltInOp0(op) =>
        render(op)

      case e @ BuiltInOp2(op) =>
        val rOp = render(op)
        val noParens = if (rOp == "as" || rOp == ":*") true else false
        if (noParens)
          s"${render(e.arg0, tableTags)} ${render(op)} ${render(e.arg1, tableTags)}"
        else
          s"(${render(e.arg0, tableTags)} ${render(op)} ${render(e.arg1, tableTags)})"

      case e @ BuiltInOp1(op) =>
        s"${renderOp1(op, e.arg0, tableTags)}"
      case e @ BuiltInOp3(op) =>
        val args = List(e.arg0, e.arg1, e.arg2).filter {
          _ match {
            case Expr.Ident(_: SingletonKey) => false
            case _                           => true
          }
        }
        if (args.size == 1) s"${render(args.head, tableTags)}.${render(op)}"
        else
          s"${render(op)}(${args.map(v => render(v, tableTags)).mkString(", ")})"

      case Expr.Ident(name) =>
        name match {
          case lang.TagKey(s)  => s"$s"
          case _: SingletonKey => ""
          case ColumnKey(tableUniqueId, columnIdx) =>
            s"$$${tableUniqueId}[${columnIdx}]"
          case Delayed(table, selection) =>
            table match {
              case ra3.tablelang.IntKey(s) =>
                s"$$T$s[${selection.fold(identity, identity)}]"
              case ra3.tablelang.TagKey(s) => s"$s"
            }
          case lang.IntKey(s) => s"$$C$s"
        }
      case DelayedIdent(Delayed(table, selection)) =>
        table match {
          case ra3.tablelang.IntKey(s) =>
            s"$$T$s[${selection.fold(identity, identity)}]"
          case ra3.tablelang.TagKey(s) =>
            s"$$T${tableTags.indexOf(s)}[${selection.fold(identity, identity)}]"
        }
      case b @ BuiltInOp2US(op) =>
        val rOp = render(op.erase)
        val noParens = if (rOp == "as" || rOp == ":*") true else false
        if (noParens)
          s"${render(b.arg0, tableTags)} ${rOp} ${render(b.arg1, tableTags)}"
        else
          s"(${render(b.arg0, tableTags)} ${rOp} ${render(b.arg1, tableTags)})"

    }

  def render[T](
      expr: TableExpr[T],
      indent: Int,
      tags: Seq[ra3.tablelang.KeyTag]
  ): String = {
    import TableExpr.*
    val padInt = (0 until indent).map(_ => "  ").mkString
    expr match {
      case ImportCsv(arg0, name, columns, maxSegmentLength, files, compression, recordSeparator, fieldSeparator, header, maxLines, bufferSize, characterDecoder, parallelism) => s"IMPORT CSV FROM ${(List(arg0) ++ files).mkString(", ")} as $columns"
      case Const(table) =>
        f"<${table.uniqueId}|${table.numRows}%,2d x${table.numCols}%,2d|${table.colNames.zip(table.columns.map(_.tag)).map { case (name, tag) => name + " " + tag }.mkString("| ")}>"
      case Ident(key) =>
        key match {
          case ra3.tablelang.IntKey(s) => s"$$T$s"
          case ra3.tablelang.TagKey(s) => s"$$T${tags.indexOf(s)}"
        }
      case Concat(a, bs, _) =>
        s"CONCAT [${render(a, indent, tags)} ${bs.map(b => render(b, indent, tags)).mkString(" ")}]"
      case Prepartition(arg0, arg1, _, _, _) =>
        s"PARTITION BY ${(List(arg0) ++ arg1).map(v => render(v, tags)).mkString(", ")}"
      case TopK(arg0, asc, k, _, _) =>
        s"TOP $k BY ${(List(arg0)).map(v => render(v, tags)).mkString(", ")} ${if (asc) "ASC" else "DESC"}"
      case Local(name, assigned, body) =>
        val rAssigned = assigned match {
          case _: Local[?, ?] | _: Tap[?] =>
            s"{\n$padInt${render(assigned, indent + 1, tags :+ name.s)}\n$padInt}"
          case _ => render(assigned, indent, tags :+ name.s)
        }
        val rLet = s"let ${render(Ident(name), 0, tags :+ name.s)} = "
        f"${padInt}$rLet$rAssigned\n$padInt${render(body, indent, tags :+ name.s)}"
      case SimpleQuery(arg0, elementwise) =>
        f"QUERY FROM ${render(arg0, 0, tags)} with ${render(elementwise, tags)}"
      case SimpleQueryCount(arg0, elementwise) =>
        f"COUNT FROM ${render(arg0, 0, tags)} with $elementwise"
      case Tap(arg0, _, _) => render(arg0, indent, tags)
      case GroupThenReduce(arg0, arg1, groupwise, _, _, _) =>
        s"GROUP-THEN-REDUCE BY ${(List(arg0) ++ arg1).map(v => render(v, tags)).mkString(", ")} with ${render(groupwise, tags)}"
      case GroupThenCount(arg0, arg1, groupwise, _, _, _) =>
        s"GROUP-THEN-COUNT BY ${(List(arg0) ++ arg1).map(v => render(v, tags)).mkString(", ")}  with ${render(groupwise, tags)}"
      case GroupPartialThenReduce(arg0, arg1, groupwise) =>
        s"PARTIAL-GROUP-THEN-REDUCE BY ${(List(arg0) ++ arg1).map(v => render(v, tags)).mkString(", ")}  with ${render(groupwise, tags)}"
      case FullTablePartialReduce(arg0, groupwise) =>
        s"PARTIAL-REDUCE  BY ${render(arg0, 0, tags)} with ${render(groupwise, tags)}"
      case ReduceTable(arg0, groupwise) =>
        s"REDUCE with $groupwise BY ${render(arg0, 0, tags)}"
      case ra3.tablelang.TableExpr.Join(arg0, arg1, _, _, _, elementwise) =>
        s"QUERY FROM JOINED ${(List(arg0) ++ arg1
            .map(_._1)).map(v => render(v, tags)).map(s => s"$s").mkString(" x ")} with ${render(elementwise, tags)}"
    }
  }
}

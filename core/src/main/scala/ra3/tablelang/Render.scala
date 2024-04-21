package ra3.tablelang
import ra3.lang._
import ra3.lang
import ra3.lang.ops.Op3._
import ra3.lang.ops.Op2._
import ra3.lang.ops.Op1._
import ra3.lang.Expr._
// import ra3.lang.ops.OpStar.MkSelect
private[ra3] object Render {

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
    case x                                 => x.toString
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
    case Cons                          => "::"
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
    case MkReturnWhere                 => "WHERE"
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
    case lang.ops.Op2.Tap              => "tap"
    case ColumnPrintfOpDcStr           => "printf"
    case MkNamedColumnSpecChunk        => "as"
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
    case x                             => x.toString()
  }
  def renderOp1(op: ra3.lang.ops.Op1, arg: ra3.lang.Expr): String = op match {
    case ColumnToInstantEpochMilliOpL => render(arg) + ".toEpochMilli"
    case MkUnnamedConstantI32         => render(arg)
    case MkUnnamedColumnSpecChunk     => render(arg)
    case MkUnnamedConstantI64         => render(arg)
    case ColumnIsMissingOpL           => render(arg) + ".isnull"
    case ColumnIsMissingOpD           => render(arg) + ".isnull"
    case ColumnRoundToIntOpD          => render(arg) + ".toInt"
    case MkUnnamedConstantF64         => render(arg)
    case ColumnToDoubleOpL            => render(arg) + ".toDouble"
    case ColumnIsMissingOpInst        => render(arg) + ".isnull"
    case ColumnNanosecondsOpInst      => render(arg) + ".nanoseconds"
    case ColumnMinutesOpInst          => render(arg) + ".minutes"
    case ColumnSecondsOpInst          => render(arg) + ".seconds"
    case ColumnYearsOpInst            => render(arg) + ".years"
    case ColumnHoursOpInst            => render(arg) + ".hours"
    case ColumnMonthsOpInst           => render(arg) + ".months"
    case ColumnDaysOpInst             => render(arg) + ".days"
    case List1                        => "list"
    case ToString                     => render(arg) + ".toString"
    case MkUnnamedConstantStr         => render(arg)
    case ColumnToISOOpInst            => render(arg) + ".toInstISO"
    case ColumnToLongOpInst           => render(arg) + ".toLong"
    case ColumnParseI64OpStr          => render(arg) + ".toLong"
    case ColumnParseInstOpStr         => render(arg) + ".toInst"
    case ColumnAbsOpI                 => render(arg) + ".abs"
    case ColumnIsMissingOpI           => render(arg) + ".isnull"
    case ColumnNotOpI                 => "not(" + render(arg) + ")"
    // case MkRawWhere                 => "WHERE " + render(arg) 
    case ColumnParseF64OpStr          => render(arg) + ".toDouble"
    case ColumnToDoubleOpInst         => render(arg) + ".toDouble"
    case ColumnIsMissingOpStr         => render(arg) + ".isnull"
    case ColumnParseI32OpStr          => render(arg) + ".toInt"
    case ColumnRoundToDayOpInst       => render(arg) + ".rounddays"
    case ColumnRoundToHourOpInst      => render(arg) + ".roundhours"
    case ColumnRoundToMonthOpInst     => render(arg) + ".roundmonths"
    case ColumnRoundToYearOpInst      => render(arg) + ".roundyears"
    case AbsOp                        => render(arg) + ".abs"
    case ColumnAbsOpD                 => render(arg) + ".abs"
    case ColumnRoundToDoubleOpD       => render(arg) + ".toDouble"
    case ColumnToDoubleOpI            => render(arg) + ".toDouble"
    case x                            => x.toString() + s"(${render(arg)})"
  }
  // def render(op: ra3.lang.ops.OpStar): String = op match {
  //   case lang.ops.OpStar.MkSelect => "SELECT"

  // }

  def render(expr: Expr): String = expr match {
    case Expr.Local(name,assigned,body) => 
      val rAssigned = assigned match {
          case _: Local =>
            s"(${render(assigned)})"
          case _ => render(assigned)
        }
        val rLet = s"${render(Ident(name))}"
        f"${render(body)} WITH ($rLet = $rAssigned)"
    case LitI64(s)           => s"${s}L"
    case LitStr(s)           => s"\"$s\""
    case BuiltInOp2(arg0, arg1, op) =>
      val rOp = render(op)
      val noParens = if (rOp == "as") true else false
      if (noParens) s"${render(arg0)} ${render(op)} ${render(arg1)}"
      else
        s"(${render(arg0)} ${render(op)} ${render(arg1)})"
    case BuiltInOp1(arg0, op) =>
      s"${renderOp1(op, arg0)}"
    case BuiltInOp3(arg0, arg1, arg2, op) =>
      val args = List(arg0, arg1, arg2).filter {
        _ match {
          case Expr.Ident(_: SingletonKey) => false
          case _                           => true
        }
      }
      if (args.size == 1) s"${render(args.head)}.${render(op)}"
      else s"${render(op)}(${args.map(render).mkString(", ")})"
    case LitNum(s) => s"$s"
    // case BuiltInOpStar(args, op) =>
    //   op match {
    //     case MkSelect =>  
    //         s"SELECT ${args.map(a => render(a)).mkString(", ")}"
    //     case _ => 
    //         s"${render(op)}(${args.map(a => render(a)).mkString(", ")})"
    //   }
      
    case Expr.Ident(name) =>
      name match {
        case lang.TagKey(_)  => ???
        case _: SingletonKey => ""
        case ColumnKey(tableUniqueId, columnIdx) =>
          s"$$${tableUniqueId}[${columnIdx}]"
        case Delayed(table, selection) =>
          table match {
            case ra3.tablelang.IntKey(s) =>
              s"$$T$s[${selection.fold(identity, identity)}]"
            case ra3.tablelang.TagKey(_) => ???
            case ra3.tablelang.TableKey(tableUniqueId) =>
              s"$$$tableUniqueId[${selection.fold(identity, identity)}]"
          }
        case lang.IntKey(s) => s"$$C$s"
      }
    case ra3.lang.Expr.Star => "*"
    case LitF64Set(s)       => s"(${s.mkString(", ")})"
    case LitStrSet(s)       => s"(${s.mkString(", ")})"
    case DelayedIdent(Delayed(table, selection)) =>
      table match {
        case ra3.tablelang.IntKey(s) =>
          s"$$T$s[${selection.fold(identity, identity)}]"
        case ra3.tablelang.TagKey(_) => ???
        case ra3.tablelang.TableKey(tableUniqueId) =>
          s"$$$tableUniqueId[${selection.fold(identity, identity)}]"
      }

    case LitI32Set(s) => s"(${s.mkString(", ")})"
    case LitF64(s)    => s"${s}d"
  }

  def render(expr: TableExpr, indent: Int): String = {
    import TableExpr._
    val padInt = (0 until indent).map(_ => "  ").mkString
    expr match {
      case Const(table) =>
        f"<${table.uniqueId}|${table.numRows}%,2d x${table.numCols}%,2d|${table.colNames.zip(table.columns.map(_.tag)).map{ case (name,tag) => name+" "+tag}.mkString("| ")}>"
      case Ident(key) =>
        key match {
          case ra3.tablelang.IntKey(s)               => s"$$T$s"
          case ra3.tablelang.TagKey(_)               => ???
          case ra3.tablelang.TableKey(tableUniqueId) => s"$$$tableUniqueId"
        }
      case Local(name, assigned, body) =>
        val rAssigned = assigned match {
          case _: Local =>
            s"{\n$padInt${render(assigned, indent + 1)}\n$padInt}"
          case _ => render(assigned, indent)
        }
        val rLet = s"let ${render(Ident(name), 0)} = "
        f"${padInt}$rLet$rAssigned\n$padInt${render(body, indent)}"
      case SimpleQuery(arg0, elementwise) =>
        f"QUERY FROM ${render(arg0, 0)} with ${render(elementwise)}"
      case SimpleQueryCount(arg0, elementwise) =>
        f"COUNT FROM ${render(arg0, 0)} with $elementwise"
      case Tap(arg0, _, tag) => f"TAP ${render(arg0, 0)} with '$tag'"
      case GroupThenReduce(arg0, arg1, groupwise, _, _, _) =>
        s"GROUP-THEN-REDUCE BY ${(List(arg0) ++ arg1).map(render).mkString(", ")} with ${render(groupwise)}"
      case GroupThenCount(arg0, arg1, groupwise, _, _, _) =>
        s"GROUP-THEN-COUNT BY ${(List(arg0) ++ arg1).map(render).mkString(", ")}  with ${render(groupwise)}"
      case GroupPartialThenReduce(arg0, arg1, groupwise) =>
        s"PARTIAL-GROUP-THEN-REDUCE BY ${(List(arg0) ++ arg1).map(render).mkString(", ")}  with ${render(groupwise)}"
      case FullTablePartialReduce(arg0, groupwise) =>
        s"PARTIAL-REDUCE  BY ${render(arg0, 0)} with ${render(groupwise)}"
      case ReduceTable(arg0, groupwise) =>
        s"REDUCE with $groupwise BY ${render(arg0, 0)}"
      case ra3.tablelang.TableExpr.Join(arg0, arg1, _, _, _, elementwise) =>
        s"JOIN ${(List(arg0) ++ arg1
            .map(_._1)).map(render).map(s => s"$s").mkString(" x ")} THEN ${render(elementwise)}"
    }
  }
}

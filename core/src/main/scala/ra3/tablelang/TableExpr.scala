package ra3.tablelang
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import tasks.TaskSystemComponents
import ra3.GroupedTable

class KeyTag
sealed trait Key
case class IntKey(s: Int) extends Key
case class TagKey(s: KeyTag) extends Key
case class TableKey(tableUniqueId: String) extends Key

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

  private[ra3] def eval(implicit
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
private[tablelang] object TableExpr {

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
  case class GroupThenReduce(
      arg0: TableExpr.Ident,
      cols: Either[Seq[String], Seq[Int]],
      partitionBase: Int,
      partitionLimit: Int,
      groupwise: ra3.lang.Expr
  ) extends TableExpr {

    val tags: Set[KeyTag] = arg0.tags
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      GroupThenReduce(
        arg0.replace(i).asInstanceOf[Ident],
        cols,
        partitionBase,
        partitionLimit,
        groupwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          table
            .groupBy(
              cols = cols match {
                case Left(value)  => value.map(s => table.colNames.indexOf(s))
                case Right(value) => value
              },
              partitionBase = partitionBase,
              partitionLimit = partitionLimit
            )
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
      arg0: TableExpr.Ident,
      arg0JoinColumn: Int,
      arg1: Seq[(TableExpr.Ident, Int, String, Int)],
      partitionBase: Int,
      partitionLimit: Int,
      elementwise: ra3.lang.Expr
  ) extends TableExpr {

    val tags: Set[KeyTag] = arg0.tags
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      Join(
        arg0.replace(i).asInstanceOf[Ident],
        arg0JoinColumn,
        arg1.map { case (t, column, how, against) =>
          (t.replace(i).asInstanceOf[Ident], column, how, against)
        },
        partitionBase,
        partitionLimit,
        elementwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          IO.parSequenceN(32)(arg1.map { case (a, b, c, d) =>
            a.evalWith(env).map(x => (x.v, b, c, d))
          }).flatMap { arg1Tables =>
            val program = elementwise
              .replaceDelayed(
                DelayedTableSchema(
                  Map(
                    arg0.key -> ((table.uniqueId, table.colNames))
                  ) ++ (arg1Tables.map(_._1) zip arg1.map(_._1)).map {
                    case (table, ident) =>
                      (ident.key, (table.uniqueId, table.colNames))
                  }
                )
              )
              .asInstanceOf[ra3.lang.Query]
            ra3.EquijoinMultipleDriver
              .equijoinMultiple(
                table,
                arg0JoinColumn,
                arg1Tables,
                partitionBase,
                partitionLimit,
                program
              )
              .map(TableValue(_))
          }

        }

    }
  }
}

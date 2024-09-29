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

private[ra3] class KeyTag
private[ra3] sealed trait Key
private[ra3] case class IntKey(s: Int) extends Key
private[ra3] case class TagKey(s: KeyTag) extends Key

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
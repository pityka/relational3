package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

case class MakeUniqueId(
    parent: String,
    tag: String,
    aux: Seq[Column[_]]
)
object MakeUniqueId {
  def queue0(
      tag: String,
      aux: Seq[Column[_]]
  )(implicit
      tsc: TaskSystemComponents
  ) =
    task(
      MakeUniqueId(
        parent = "",
        tag = tag,
        aux = aux
      )
    )(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    )
  def queue(
      parent: Table,
      tag: String,
      aux: Seq[Column[_]]
  )(implicit
      tsc: TaskSystemComponents
  ) =
    task(
      MakeUniqueId(
        parent = parent.uniqueId,
        tag = tag,
        aux = aux
      )
    )(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    )
  def queue2(
      parent1: Table,
      parent2: Table,
      tag: String,
      aux: Seq[Column[_]]
  )(implicit
      tsc: TaskSystemComponents
  ) =
    task(
      MakeUniqueId(
        parent = parent1.uniqueId+"-"+parent2.uniqueId,
        tag = tag,
        aux = aux
      )
    )(
      ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
    )
  implicit val codec: JsonValueCodec[MakeUniqueId] = JsonCodecMaker.make
  val task = Task[MakeUniqueId, String]("MakeUniqueId", 1) { case _ =>
    _ => IO.delay(java.util.UUID.randomUUID().toString)

  }
}

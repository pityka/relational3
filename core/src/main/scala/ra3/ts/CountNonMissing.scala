package ra3.ts

import ra3._
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

private[ra3] case class CountNonMissing(
    input: Seq[Segment]
)
private[ra3] object CountNonMissing {
  def doit(input: Seq[Segment])(implicit tsc: TaskSystemComponents) = {
    input.foldLeft(IO.pure(0L)) { case (acc, next) =>
      acc.flatMap { acc =>
        next.buffer.map(_.countNonMissing + acc)
      }
    }
  }
  def queue(input: Seq[Segment])(implicit
      tsc: TaskSystemComponents
  ): IO[Long] =
    task(CountNonMissing(input))(
      ResourceRequest(cpu = (1, 1), memory = 100, scratch = 0, gpu = 0)
    )
  implicit val codec: JsonValueCodec[CountNonMissing] = JsonCodecMaker.make
  val task = Task[CountNonMissing, Long]("CountNonMissing", 1) { case input =>
    implicit ce => doit(input.input)

  }
}

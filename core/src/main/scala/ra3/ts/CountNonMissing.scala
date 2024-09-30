package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class CountNonMissing(
    tag: ColumnTag,
    input: Seq[Segment]
)
private[ra3] object CountNonMissing {
  def doit(
      tag: ColumnTag
  )(input: Seq[tag.SegmentType])(implicit tsc: TaskSystemComponents) = {
    input.foldLeft(IO.pure(0L)) { case (acc, next) =>
      acc.flatMap { acc =>
        tag.buffer(next).map(_.countNonMissing + acc)
      }
    }
  }
  def queue(tag: ColumnTag)(input: Seq[tag.SegmentType])(implicit
      tsc: TaskSystemComponents
  ): IO[Long] =
    task(CountNonMissing(tag, input))(
      ResourceRequest(cpu = (1, 1), memory = 100, scratch = 0, gpu = 0)
    )
  implicit val codec: JsonValueCodec[CountNonMissing] = JsonCodecMaker.make
  val task = Task[CountNonMissing, Long]("CountNonMissing", 1) { case input =>
    implicit ce =>
      doit(input.tag)(input.input.map(_.asInstanceOf[input.tag.SegmentType]))

  }
}

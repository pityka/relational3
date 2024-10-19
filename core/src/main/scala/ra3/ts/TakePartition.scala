package ra3.ts

import ra3.*
import tasks.*
import tasks.jsonitersupport.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import cats.effect.IO

private[ra3] case class TakePartition(
    inputSegmentsWithPartitionMaps: Seq[(TaggedSegment, SegmentInt)],
    numPartition: Int,
    outputPath: LogicalPath
)
private[ra3] object TakePartition {
  private def doit(
      inputSegmentsWithPartitionMaps: Seq[(TaggedSegment, SegmentInt)],
      numPartition: Int,
      outputPath: LogicalPath
  )(implicit tsc: TaskSystemComponents): IO[Vector[Segment]] = {
    scribe.debug(
      s"Take $numPartition partitions from ${inputSegmentsWithPartitionMaps.size} segments of ${outputPath.table}/${outputPath.column} to $outputPath"
    )
    assert(outputPath.partition.isDefined)
    assert(outputPath.partition.get.numPartitions == numPartition)
    assert(inputSegmentsWithPartitionMaps.nonEmpty)
    val allPartitionsBuffered: IO[Vector[Vector[TaggedBuffer]]] =
      inputSegmentsWithPartitionMaps.foldLeft(
        IO.pure(
          0 until numPartition map (_ => Vector.empty[TaggedBuffer]) toVector
        )
      ) { case (alreadyDone, (nextSegment, nextPartitionMap)) =>
        alreadyDone.flatMap { alreadyDone =>
          IO.both(
            nextSegment.tag.buffer(nextSegment.segment),
            nextPartitionMap.buffer
          ).map { case (bufferedSegment, bufferedPartitionMap) =>
            val partitionsOfNextSegment =
              nextSegment.tag.partition(
                bufferedSegment,
                numPartition,
                bufferedPartitionMap
              )
            assert(alreadyDone.size == partitionsOfNextSegment.size)
            (alreadyDone zip partitionsOfNextSegment).map { case (vec, buf) =>
              vec.appended(nextSegment.tag.makeTaggedBuffer(buf))
            }

          }
        }
      }

    allPartitionsBuffered
      .flatMap { allPartitionsBuffered =>
        IO.parSequenceN(32)(allPartitionsBuffered.zipWithIndex.map {
          case (buffers, pIdx) =>
            val tag = buffers.head.tag
            assert(buffers.forall(_.tag == tag))
            tag.toSegment(
              tag
                .cat(buffers.map(_.buffer.asInstanceOf[tag.BufferType])*),
              outputPath.copy(
                partition =
                  Some(outputPath.partition.get.copy(partitionId = pIdx))
              )
            )

        })
      }
      .map { partitions =>
        assert(partitions.size == numPartition)
        partitions
      }

  }
  def queue(
      inputSegmentsWithPartitionMaps: Seq[(TaggedSegment, SegmentInt)],
      numPartition: Int,
      outputPath: LogicalPath
  )(implicit
      tsc: TaskSystemComponents
  ): IO[Seq[Segment]] =
   IO {
      scribe.debug(
        s"Queueing TakePartition partition idx: ${numPartition} segments: ${inputSegmentsWithPartitionMaps.size}"
      )
    } *>task(
    TakePartition(
      inputSegmentsWithPartitionMaps = inputSegmentsWithPartitionMaps,
      numPartition = numPartition,
      outputPath = outputPath
    )
  )(
    ResourceRequest(
      cpu = (1, 1),
      memory = inputSegmentsWithPartitionMaps
        .map(v => ra3.Utils.guessMemoryUsageInMB(v._1.segment) * 2)
        .sum,
      scratch = 0,
      gpu = 0
    )
  )
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[TakePartition] = JsonCodecMaker.make
  implicit val codecOut: JsonValueCodec[Seq[Segment]] = JsonCodecMaker.make
  // $COVERAGE-ON$
  val task = Task[TakePartition, Seq[Segment]]("takepartition", 1) {
    case input =>
      implicit ce =>
        doit(
          input.inputSegmentsWithPartitionMaps,
          input.numPartition,
          input.outputPath
        )

  }
}

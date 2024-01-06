package ra3

import cats.effect.IO

import ra3.ts.EstimateCDF
import ra3.ts.MakeUniqueId
import ra3.ts.MergeCDFs
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import tasks._

object Column {
  case class Int32Column(segments: Vector[SegmentInt]) extends Column {
    type Self = Int32Column
    type DType = Int32.type
  val dataType = Int32
}
case class F64Column(segments: Vector[SegmentDouble]) extends Column {
  type Self = F64Column
  type DType = F64.type
  val dataType = F64
}
  implicit val codec: JsonValueCodec[Column] = JsonCodecMaker.make
  
  def apply[D <: DataType](tpe0: D)(segments0: Vector[tpe0.SegmentType]) : D#ColumnType  =
   tpe0.makeColumn(segments0)

  def cast[D <: DataType](tpe: D)(segments: Vector[Segment]): D#ColumnType =
    tpe.makeColumn(segments.map(tpe.cast))
}



sealed trait Column extends ColumnOps { self => 
  type DType <: DataType
  val dataType : DType
  type Elem = dataType.Elem
  def segments: Vector[dataType.SegmentType]
  // val z : Segment[DType] = dataType.cast[DType](???)

  def castAndConcatenate(other: Column) = ++(other.asInstanceOf[self.type])
  def ++(other: self.type) = {
  
    val both : Vector[dataType.SegmentType]= segments ++ other.segments
    Column(dataType)(both)
  }

  def estimateCDF(coverage: Double, numPointsPerSegment: Int)(implicit
      tsc: TaskSystemComponents
  ) = {
    assert(coverage > 0d)
    assert(coverage <= 1d)
    val total = segments.map(_.numElems.toLong).sum
    val numPick = math.min(1, (coverage * total).toLong)
    val shuffleSegments = scala.util.Random.shuffle(segments.zipWithIndex)
    val cumulative = shuffleSegments
      .map(v => (v, v._1.numElems))
      .scanLeft(0)((a, b) => (a + b._2))
    val pickSegments =
      shuffleSegments.zip(cumulative).takeWhile(_._2 < numPick).map(_._1)
    assert(pickSegments.size > 0)
    assert(pickSegments.size <= segments.size)
    MakeUniqueId.queue0("estimatecdf", List(this)).flatMap { uniqueId =>
      IO.parSequenceN(32)(pickSegments.map { case (segment, segmentIdx) =>
        EstimateCDF.queue(
          segment,
          numPointsPerSegment,
          LogicalPath(
            table = uniqueId,
            partition = None,
            segment = segmentIdx,
            column = 0
          )
        )

      }).flatMap { cdfs =>
        MergeCDFs.queue(
          cdfs,
          LogicalPath(
            table = uniqueId + ".merged",
            partition = None,
            segment = 0,
            column = 0
          )
        )
      }
    }
  }
} 
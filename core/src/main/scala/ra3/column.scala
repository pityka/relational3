package ra3

import cats.effect.IO

import ra3.ts.EstimateCDF
import ra3.ts.MakeUniqueId
import ra3.ts.MergeCDFs
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import tasks._
import bufferimpl.CharSequenceOrdering
import columnimpl._

object Column {
  case class Int32Column(segments: Vector[SegmentInt])
      extends Column
      with I32ColumnImpl {
    def minMax: Option[(Int, Int)] = {
      val s = segments.flatMap(_.minMax.toSeq)
      if (s.isEmpty) None
      else Some((s.map(_._1).min, s.map(_._2).max))
    }
    override def ++(other: Int32Column): Int32Column = Int32Column(
      segments ++ other.segments
    )

    type Elem = Int
    type BufferType = BufferInt
    type SegmentType = SegmentInt
    type ColumnType = Int32Column
    type ColumnTagType = ColumnTag.I32.type
    def tag = ColumnTag.I32
  }
  case class I64Column(segments: Vector[SegmentLong]) extends Column {
    def minMax: Option[(Long, Long)] = {
      val s = segments.flatMap(_.minMax.toSeq)
      if (s.isEmpty) None
      else Some((s.map(_._1).min, s.map(_._2).max))
    }
    override def ++(other: I64Column): I64Column = I64Column(
      segments ++ other.segments
    )

    type Elem = Long
    type BufferType = BufferLong
    type SegmentType = SegmentLong
    type ColumnType = I64Column
    type ColumnTagType = ColumnTag.I64.type
    def tag = ColumnTag.I64
  }
  case class InstantColumn(segments: Vector[SegmentInstant]) extends Column {
    def minMax: Option[(Long, Long)] = {
      val s = segments.flatMap(_.minMax.toSeq)
      if (s.isEmpty) None
      else Some((s.map(_._1).min, s.map(_._2).max))
    }
    override def ++(other: InstantColumn): InstantColumn = InstantColumn(
      segments ++ other.segments
    )

    type Elem = Long
    type BufferType = BufferInstant
    type SegmentType = SegmentInstant
    type ColumnType = InstantColumn
    type ColumnTagType = ColumnTag.Instant.type
    def tag = ColumnTag.Instant
  }
  case class StringColumn(segments: Vector[SegmentString]) extends Column {
    def minMax: Option[(String, String)] = {
      val s = segments.flatMap(_.minMax.toSeq)
      if (s.isEmpty) None
      else
        Some(
          (
            s.map(_._1).min(CharSequenceOrdering),
            s.map(_._2).max(CharSequenceOrdering)
          )
        )
    }
    override def ++(other: StringColumn): StringColumn = StringColumn(
      segments ++ other.segments
    )

    type Elem = CharSequence
    type BufferType = BufferString
    type SegmentType = SegmentString
    type ColumnType = StringColumn
    type ColumnTagType = ColumnTag.StringTag.type
    def tag = ColumnTag.StringTag
  }
  case class F64Column(segments: Vector[SegmentDouble])
      extends Column
      with F64ColumnImpl {
    def minMax: Option[(Double, Double)] = {
      val s = segments.flatMap(_.minMax.toSeq)
      if (s.isEmpty) None
      else Some((s.map(_._1).min, s.map(_._2).max))
    }
    override def ++(other: F64Column): F64Column = F64Column(
      segments ++ other.segments
    )

    type Elem = Double
    type BufferType = BufferDouble
    type SegmentType = SegmentDouble
    type ColumnType = F64Column
    type ColumnTagType = ColumnTag.F64.type
    def tag = ColumnTag.F64
  }
  implicit val codec: JsonValueCodec[Column] = JsonCodecMaker.make

}

sealed trait Column extends ColumnOps { self =>
  type ColumnType >: this.type <: Column {
    type Elem = self.Elem
  }
  type Elem
  type BufferType <: Buffer {
    type Elem = self.Elem
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
  }
  type SegmentType <: Segment {
    type Elem = self.Elem
    type BufferType = self.BufferType
    type SegmentType = self.SegmentType
  }
  type ColumnTagType <: ColumnTag {
    type ColumnType = self.ColumnType
    type SegmentType = self.SegmentType
    type BufferType = self.BufferType
    type Elem = self.Elem
  }
  def tag: ColumnTagType
  def segments: Vector[SegmentType]
  def ++(other: ColumnType): ColumnType
  def minMax: Option[(Elem, Elem)]

  override def toString =
    s"$tag\tN_segments=${segments.size}\tN_elem=${segments.map(_.numElems).sum}"

  def as(c: Column) = this.asInstanceOf[c.ColumnType]
  def castAndConcatenate(other: Column) = ++(other.asInstanceOf[ColumnType])

  def estimateCDF(coverage: Double, numPointsPerSegment: Int)(implicit
      tsc: TaskSystemComponents
  ) = {
    assert(coverage > 0d)
    assert(coverage <= 1d)
    val total = segments.map(_.numElems.toLong).sum
    val numPick = math.max(1, (coverage * total).toLong)
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

  def elementwise[
      C2 <: Column { type ColumnType = C2 },
      OP <: ra3.ts.BinaryOpTag {
        type SegmentTypeA = SegmentType; type SegmentTypeB = C2#SegmentType
      }
  ](
      other: C2,
      opTag: OP
  )(implicit tsc: TaskSystemComponents) = {
    assert(self.segments.size == other.segments.size)
    IO.parSequenceN(math.min(32, self.segments.size))(
      self.segments.zip(other.segments).zipWithIndex.map {
        case ((a, b), segmentIdx) =>
          assert(a.numElems == b.numElems)
          ra3.ts.ElementwiseBinaryOperation
            .queue(opTag.op(a, b), LogicalPath(???, None, segmentIdx, 0))
      }
    ).map(segments => opTag.tagC.makeColumn(segments))
  }
}

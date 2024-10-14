package ra3

import cats.effect.IO

import ra3.ts.EstimateCDF
import ra3.ts.MakeUniqueId
import ra3.ts.MergeCDFs
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*
import tasks.*
import bufferimpl.CharSequenceOrdering
import ra3.Column.Int32Column

private[ra3] object Column {

  def estimateCDF(
      tag: ColumnTag
  )(column: tag.ColumnType, coverage: Double, numPointsPerSegment: Int)(implicit
      tsc: TaskSystemComponents
  ) = {
    assert(coverage > 0d)
    assert(coverage <= 1d)
    val segm = tag.segments(column)
    val total = segm.map(_.numElems.toLong).sum
    val numPick = math.max(1, (coverage * total).toLong)
    val shuffleSegments =
      new scala.util.Random(42).shuffle(segm.zipWithIndex)
    val cumulative = shuffleSegments
      .map(v => (v, v._1.numElems))
      .scanLeft(0)((a, b) => (a + b._2))
    val pickSegments =
      shuffleSegments.zip(cumulative).takeWhile(_._2 < numPick).map(_._1)
    assert(pickSegments.size > 0)
    assert(pickSegments.size <= segm.size)
    ra3.ts.MakeUniqueId.queue0("estimatecdf", List(column)).flatMap {
      uniqueId =>
        IO.parSequenceN(32)(pickSegments.map { case (segment, segmentIdx) =>
          ra3.ts.EstimateCDF.queue(tag)(
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
          ra3.ts.MergeCDFs.queue(tag)(
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
  case class Int32Column(segments: Vector[SegmentInt])
      extends Column
      with TaggedColumn {
    def column = this
    val tag: ColumnTag.I32.type = ColumnTag.I32
    def nonMissingMinMax: Option[(Int, Int)] = {
      val s = segments.flatMap(_.nonMissingMinMax.toSeq)
      if (s.isEmpty) None
      else Some((s.map(_._1).min, s.map(_._2).max))
    }

    type Elem = Int
    type BufferType = BufferInt
    type SegmentType = SegmentInt
    type ColumnType = Int32Column
    type ColumnTagType = ColumnTag.I32.type
  }
  case class I64Column(segments: Vector[SegmentLong])
      extends Column
      with TaggedColumn {
    def column = this
    def nonMissingMinMax: Option[(Long, Long)] = {
      val s = segments.flatMap(_.nonMissingMinMax.toSeq)
      if (s.isEmpty) None
      else Some((s.map(_._1).min, s.map(_._2).max))
    }
    def ++(other: I64Column): I64Column = I64Column(
      segments ++ other.segments
    )

    type Elem = Long
    type BufferType = BufferLong
    type SegmentType = SegmentLong
    type ColumnType = I64Column
    type ColumnTagType = ColumnTag.I64.type
    val tag: ColumnTag.I64.type = ColumnTag.I64
  }
  case class InstantColumn(segments: Vector[SegmentInstant])
      extends Column
      with TaggedColumn {
    def column = this
    def nonMissingMinMax: Option[(Long, Long)] = {
      val s = segments.flatMap(_.nonMissingMinMax.toSeq)
      if (s.isEmpty) None
      else Some((s.map(_._1).min, s.map(_._2).max))
    }
    def ++(other: InstantColumn): InstantColumn = InstantColumn(
      segments ++ other.segments
    )

    type Elem = Long
    type BufferType = BufferInstant
    type SegmentType = SegmentInstant
    type ColumnType = InstantColumn
    type ColumnTagType = ColumnTag.Instant.type
    val tag: ColumnTag.Instant.type = ColumnTag.Instant
  }
  case class StringColumn(segments: Vector[SegmentString])
      extends Column
      with TaggedColumn {
    def column = this
    def nonMissingMinMax: Option[(String, String)] = {
      val s = segments.flatMap(_.nonMissingMinMax.toSeq)
      if (s.isEmpty) None
      else
        Some(
          (
            s.map(_._1).min(CharSequenceOrdering).toString,
            s.map(_._2).max(CharSequenceOrdering).toString
          )
        )
    }
    def ++(other: StringColumn): StringColumn = StringColumn(
      segments ++ other.segments
    )

    type Elem = CharSequence
    type BufferType = BufferString
    type SegmentType = SegmentString
    type ColumnType = StringColumn
    type ColumnTagType = ColumnTag.StringTag.type
    val tag: ColumnTag.StringTag.type = ColumnTag.StringTag
  }
  case class F64Column(segments: Vector[SegmentDouble])
      extends Column
      with TaggedColumn {
    def column = this
    def nonMissingMinMax: Option[(Double, Double)] = {
      val s = segments.flatMap(_.nonMissingMinMax.toSeq)
      if (s.isEmpty) None
      else Some((s.map(_._1).min, s.map(_._2).max))
    }
    def ++(other: F64Column): F64Column = F64Column(
      segments ++ other.segments
    )

    type Elem = Double
    type BufferType = BufferDouble
    type SegmentType = SegmentDouble
    type ColumnType = F64Column
    type ColumnTagType = ColumnTag.F64.type
    val tag: ColumnTag.F64.type = ColumnTag.F64
  }
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[Column] = JsonCodecMaker.make
  // $COVERAGE-ON$

}

private[ra3] sealed trait TaggedColumn { self =>
  type ColumnType <: Column
  val tag: ColumnTag {
    type ColumnType = self.ColumnType
  }
  def column: tag.ColumnType
  def segments: Vector[tag.SegmentType]
  def castAndConcatenate(other: TaggedColumn) = tag.makeTaggedColumn(
    tag.castAndConcatenate(
      this.column,
      (other.column: Column).asInstanceOf[tag.ColumnType]
    )
  )
}

private[ra3] object TaggedColumn {
  // $COVERAGE-OFF$
  implicit val codec: JsonValueCodec[TaggedColumn] = JsonCodecMaker.make
  // $COVERAGE-ON$

  case class TaggedColumnI32(column: Column.Int32Column) extends TaggedColumn {
    type ColumnType = Column.Int32Column
    val tag: ColumnTag.I32.type = ColumnTag.I32
    def segments = column.segments
  }
  case class TaggedColumnI64(column: Column.I64Column) extends TaggedColumn {
    type ColumnType = Column.I64Column
    val tag: ColumnTag.I64.type = ColumnTag.I64
    def segments = column.segments
  }
  case class TaggedColumnF64(column: Column.F64Column) extends TaggedColumn {
    type ColumnType = Column.F64Column
    val tag: ColumnTag.F64.type = ColumnTag.F64
    def segments = column.segments
  }
  case class TaggedColumnString(column: Column.StringColumn)
      extends TaggedColumn {
    type ColumnType = Column.StringColumn
    val tag: ColumnTag.StringTag.type = ColumnTag.StringTag
    def segments = column.segments
  }
  case class TaggedColumnInstant(column: Column.InstantColumn)
      extends TaggedColumn {
    type ColumnType = Column.InstantColumn
    val tag: ColumnTag.Instant.type = ColumnTag.Instant
    def segments = column.segments
  }

}

private[ra3] sealed trait Column { self =>

  type Elem

  // override def toString =
  //   s"$tag\tN_segments=${segments.size}\tN_elem=${segments.map(_.numElems).sum}"

}

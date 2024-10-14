package ra3
import tasks.TaskSystemComponents
import cats.effect.IO

trait TableExtensions {
  extension (self: BufferedTable) {
    def toFrame = self.toStringFrame
    def toHomogeneousFrameWithRowIndex[Rx, V](rowIndexCol: Int)(implicit
        st: org.saddle.ST[V],
        st2: org.saddle.ST[Rx],
        ord: org.saddle.ORD[Rx]
    ) = {
      import org.saddle.*
      val header = self.columns(rowIndexCol).toSeq.asInstanceOf[Seq[Rx]].toVec

      Frame(
        self.columns.zipWithIndex
          .filterNot(v => v._2 != rowIndexCol)
          .map(_._1)
          .map { col =>
            col.toSeq.asInstanceOf[Seq[V]].toVec
          }*
      ).setRowIndex(Index(header.toArray))

    }

    def toHomogeneousFrame(
        tag: ColumnTag
    )(implicit st: org.saddle.ST[tag.Elem]) = {
      import org.saddle.*
      Frame(self.columns.map(_.toSeq.asInstanceOf[Seq[tag.Elem]].toVec)*)
        .setColIndex(self.colNames.toIndex)
    }
    // def toStringFrame = {
    //   import org.saddle.*
    //   Frame(self.columns.map { buffer =>
    //     buffer match {
    //       case buffer: BufferInstant =>
    //         buffer
    //           // .as(ra3.ColumnTag.Instant)
    //           .toSeq
    //           .map(v => java.time.Instant.ofEpochMilli(v).toString)
    //           .toVec
    //       case _ => {
    //         val ar = Array.ofDim[String](buffer.length)
    //         var i = 0
    //         val n = ar.length
    //         while (i < n) {
    //           if (!buffer.isMissing(i)) {
    //             ar(i) = buffer.elementAsCharSequence(i).toString
    //           }
    //           i += 1
    //         }
    //         ar.toVec
    //       }
    //     }
    //   }*)
    //     .setColIndex(self.colNames.toIndex)
    // }
  }
  extension (self: Table) {

    /** This is almost noop, select columns
      *
      * @param others
      * @return
      */
    def selectColumns(
        columnIndexes: Int*
    )(implicit tsc: TaskSystemComponents): IO[Table] = {
      val name = ra3.ts.MakeUniqueId.queue(
        self,
        s"select-columns-${columnIndexes.mkString("_")}",
        Nil
      )
      name.map { name =>
        val cols = columnIndexes map { cIdx =>
          self.columns(cIdx)
        } toVector

        Table(
          cols,
          columnIndexes.map(self.colNames).toVector,
          name,
          None // could keep it, but the column index would shift away. Need stable column identifier
        )
      }
    }

    // /** Concat list of columns */
    def addColOfSameSegmentation(c: TaggedColumn, colName: String)(implicit
        tsc: TaskSystemComponents
    ): IO[Table] = {
      assert(c.tag.segments(c.column).map(_.numElems) == self.segmentation)
      val name = ra3.ts.MakeUniqueId.queue(
        self,
        s"addcol-$colName",
        List(c.column)
      )
      name.map(name =>
        self.copy(
          columns = self.columns.appended(c),
          colNames = self.colNames.appended(colName),
          uniqueId = name
        )
      )
    }
    def addColumnFromSeq(tag: ColumnTag, columnName: String)(
        elems: Seq[tag.Elem]
    )(implicit tsc: TaskSystemComponents): IO[Table] = {
      assert(self.numRows == elems.length.toLong)
      val idx = self.columns.size
      val segmented = self.segmentation
        .foldLeft((elems, Vector.empty[Seq[tag.Elem]])) {
          case ((rest, acc), size) =>
            rest.drop(size) -> (acc :+ rest.take(size))
        }
        ._2
        .map(_.toSeq)
      val col = tag
        .makeColumnFromSeq(self.uniqueId, idx)(segmented)
        .map(tag.makeTaggedColumn)
      col.flatMap { col =>
        self.addColOfSameSegmentation(col, columnName)
      }
    }

    /**   - Align predicate segment with table segmentation
      *   - For each aligned predicate segment, buffer it
      *   - For each column
      *   - For each segment in the column
      *   - Buffer column segment
      *   - Apply buffered predicate segment to buffered column segment
      *   - Write applied buffer to local segment
      *   - Resegment
      *
      * Variant which takes BufferedTable => BufferInt
      *
      * @param predicate
      * @return
      */
    def rfilter(
        predicate: TaggedColumn
    )(implicit tsc: TaskSystemComponents): IO[Table] = {
      assert(
        self.columns.head.segments.size == predicate.tag
          .segments(predicate.column)
          .size
      )
      ts.MakeUniqueId.queue(self, "rfilter", List(predicate.column)).flatMap {
        name =>
          IO.parTraverseN(math.min(32, self.columns.size))(
            self.columns.zipWithIndex
          ) { case (column, columnIdx) =>
            IO.parTraverseN(
              math.min(32, predicate.tag.segments(predicate.column).size)
            )(
              predicate.tag.segments(predicate.column).zipWithIndex
            ) { case (segment, segmentIdx) =>
              ts.Filter.queue(tag = column.tag, predicateTag = predicate.tag)(
                input = column.segments(segmentIdx),
                predicate = segment,
                outputPath = LogicalPath(name, None, segmentIdx, columnIdx)
              )
            }.map(segments =>
              column.tag.makeTaggedColumn(column.tag.makeColumn(segments))
            )

          }.map(columns =>
            Table(
              columns,
              self.colNames,
              name,
              self.partitions
            )
          )
      }
    }

    /**   - For each aligned index segment, buffer it
      *   - For each column
      *   - For each segment in the column
      *   - Buffer column segment
      *   - Apply buffered predicate segment to buffered column segment
      *   - Write applied buffer to segment and upload
      *
      * @param indexes
      *   for each segment
      * @return
      */
    def take(
        indexes: Column.Int32Column
    )(implicit tsc: TaskSystemComponents): IO[Table] = {
      assert(self.columns.head.segments.size == indexes.segments.size)
      ts.MakeUniqueId.queue(self, "take", List(indexes)).flatMap { name =>
        IO.parTraverseN(math.min(32, self.columns.size))(
          self.columns.zipWithIndex
        ) { case (column, columnIdx) =>
          IO.parTraverseN(math.min(32, indexes.segments.size))(
            indexes.segments.zipWithIndex
          ) { case (segment, segmentIdx) =>
            ts.TakeIndex.queue(tag = column.tag)(
              input = column.segments(segmentIdx),
              idx = segment,
              outputPath = LogicalPath(name, None, segmentIdx, columnIdx)
            )
          }.map { segments =>
            column.tag.makeTaggedColumn(column.tag.makeColumn(segments))
          }

        }.map(columns =>
          Table(
            columns,
            self.colNames,
            name,
            self.partitions
          )
        )
      }
    }
  }
}

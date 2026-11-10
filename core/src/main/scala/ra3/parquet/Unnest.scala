package ra3.parquet

import Parquet.*

private[ra3] object Unnest {

  final case class SchemaNode(
      name: String,
      repetition: Int,
      ptype: Option[Int],
      convertedType: Int,
      children: Vector[SchemaNode]
  )

  def buildTree(schema: Vector[SchemaRaw]): SchemaNode = {
    var idx = 0
    def build(): SchemaNode = {
      val e = schema(idx); idx += 1
      val kids = (0 until e.numChildren).map(_ => build()).toVector
      SchemaNode(e.name, e.repetition, e.ptype, e.convertedType, kids)
    }
    build()
  }

  final case class LeafInfo(
      path: Vector[String],
      ptype: Int,
      convertedType: Int,
      maxRep: Int,
      maxDef: Int,
      elementExistsLevel: Int
  )

  def leaves(root: SchemaNode): Vector[LeafInfo] = {
    val acc = scala.collection.mutable.ArrayBuffer.empty[LeafInfo]
    def walk(
        node: SchemaNode,
        path: Vector[String],
        rep: Int,
        defl: Int,
        repAncestorDef: Int
    ): Unit = {
      val (r2, d2) = node.repetition match {
        case REP_OPTIONAL => (rep, defl + 1)
        case REP_REPEATED => (rep + 1, defl + 1)
        case _            => (rep, defl)
      }
      val repAnc2 = if (node.repetition == REP_REPEATED) d2 else repAncestorDef
      if (node.children.isEmpty)
        acc += LeafInfo(
          path,
          node.ptype.getOrElse(-1),
          node.convertedType,
          r2,
          d2,
          repAnc2
        )
      else node.children.foreach(c => walk(c, path :+ c.name, r2, d2, repAnc2))
    }
    root.children.foreach(c => walk(c, Vector(c.name), 0, 0, 0))
    acc.toVector
  }

  def assembleAuto(bytes: Array[Byte]): Vector[ReadColumn] = {
    val footer = Parquet.readFooter(bytes)
    val allLeaves = leaves(buildTree(footer.schema))
    val listRoots = allLeaves.filter(_.maxRep >= 1).map(_.path.head).distinct
    listRoots.length match {
      case 0 => Parquet.readFile(bytes)
      case 1 => assemble(bytes, footer, listRoots.head)
      case _ =>
        throw new RuntimeException(
          s"Parquet file has multiple list columns (${listRoots.mkString(", ")}); automatic unnest supports a single list column."
        )
    }
  }

  def assemble(
      bytes: Array[Byte],
      footer: Footer,
      listColumn: String
  ): Vector[ReadColumn] = {
    val allLeaves = leaves(buildTree(footer.schema))
    val flatLeaves = allLeaves.filter(_.maxRep == 0)
    val listLeaves =
      allLeaves.filter(l => l.path.head == listColumn && l.maxRep >= 1)
    if (listLeaves.isEmpty)
      throw new RuntimeException(
        s"scanParquet: no repeated (list) column named '$listColumn' found"
      )
    if (!listLeaves.forall(_.maxRep == 1))
      throw new RuntimeException(
        s"unnestParquet supports only single-level lists (max repetition 1); '$listColumn' is more deeply nested"
      )
    val elementExistsLevel = listLeaves.head.elementExistsLevel
    if (!listLeaves.forall(_.elementExistsLevel == elementExistsLevel))
      throw new RuntimeException(
        s"unnestParquet: inconsistent list structure for '$listColumn'"
      )

    val flatOut =
      flatLeaves.map(_ => scala.collection.mutable.ArrayBuffer.empty[Option[Any]])
    val listOut =
      listLeaves.map(_ => scala.collection.mutable.ArrayBuffer.empty[Option[Any]])

    def metaFor(rg: Vector[PColMeta], leaf: LeafInfo): PColMeta =
      rg.find(_.path == leaf.path)
        .getOrElse(
          throw new RuntimeException(
            s"column ${leaf.path.mkString(".")} not found in row group"
          )
        )

    footer.rowGroups.indices.foreach { rgIdx =>
      val rg = footer.rowGroups(rgIdx)
      val parentRows = footer.rowGroupRows(rgIdx)
      val flatDecoded = flatLeaves.map(l => decodeChunk(bytes, metaFor(rg, l)))
      val listDecoded =
        listLeaves.map(l => decodeNestedChunk(bytes, metaFor(rg, l), l.maxRep, l.maxDef))
      val reps = listDecoded.head.map(_._1)
      val n = reps.length
      val starts = (0 until n).filter(i => reps(i) == 0).toVector
      if (starts.length != parentRows)
        throw new RuntimeException(
          s"unnestParquet: expected $parentRows parent rows, found ${starts.length}"
        )
      starts.indices.foreach { p =>
        val s = starts(p)
        val e = if (p + 1 < starts.length) starts(p + 1) else n
        val isEmpty =
          (e - s == 1) && listDecoded.head(s)._2 < elementExistsLevel
        if (!isEmpty) {
          var j = s
          while (j < e) {
            flatLeaves.indices.foreach(fi => flatOut(fi) += flatDecoded(fi)(p))
            listLeaves.indices.foreach(li => listOut(li) += listDecoded(li)(j)._3)
            j += 1
          }
        }
      }
    }

    val flatCols = flatLeaves.zipWithIndex.map { case (l, fi) =>
      ReadColumn(l.path.last, l.ptype, l.convertedType, flatOut(fi).toVector)
    }
    val listCols = listLeaves.zipWithIndex.map { case (l, li) =>
      ReadColumn(l.path.last, l.ptype, l.convertedType, listOut(li).toVector)
    }
    (flatCols ++ listCols).toVector
  }
}

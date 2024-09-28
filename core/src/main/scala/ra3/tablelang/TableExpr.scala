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

private[ra3] class KeyTag
private[ra3] sealed trait Key
private[ra3] case class IntKey(s: Int) extends Key
private[ra3] case class TagKey(s: KeyTag) extends Key
private[ra3] case class TableKey(tableUniqueId: String) extends Key

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

sealed trait TableExpr { self =>

  type T

  def render: String = Render.render(self, 0)

  def evaluate(implicit
      tsc: TaskSystemComponents
  ): IO[ra3.Table] = evalWith(Map.empty).map(_.v)

  private[tablelang] def evalWith(env: Map[Key, TableValue])(implicit
      tsc: TaskSystemComponents
  ): IO[TableValue]

  private[ra3] def hash = {
    val bytes = writeToArray(this.replaceTags())
    com.google.common.hash.Hashing.murmur3_128().hashBytes(bytes).asLong()
  }
  // utilities for analysis of the tree
  private[tablelang] def tags: Set[KeyTag]
  private[tablelang] def replace(map: Map[KeyTag, Int]): TableExpr
  private[ra3] def replaceTags() =
    replace(this.tags.toSeq.zipWithIndex.toMap)

  

  inline def in[R](
      body: TableExpr.Ident { type T = self.T } => TableExpr{type T = R}
  ): TableExpr{type T = R} =
    ra3.tablelang.localR(self)(i => body(i))

  

}
object TableExpr {

  implicit class ColumnSyntax1[T0](a: TableExpr {
    type T = ConstMarker[T0]
  }) {
    def columns[R](
        body: DelayedIdent[T0] => TableExpr{type T = R}
    ): TableExpr{type T = R} =
       a.in { t =>
        t[T0](0) { case (c0) =>
          body(c0)
        }
      }

  }
  implicit class ColumnSyntax2[T0,T1](a: TableExpr {
    type T = ConstMarker[(T0,T1)]
  }) {
    def columns[R](
        body: (
          DelayedIdent[T0],
          DelayedIdent[T1]
          ) => TableExpr{type T = R}
    ): TableExpr{type T = R} =
       a.in { (t) =>
        t[T0,T1](0,1) { case (c0,c1) =>
          body(c0,c1)
        }
      }

  }
  implicit class ColumnSyntax3[T0,T1,T2](a: TableExpr {
    type T = ConstMarker[(T0,T1,T2)]
  }) {
    def columns[R](
        body: (
          DelayedIdent[T0],
          DelayedIdent[T1],
          DelayedIdent[T2],
          ) => TableExpr{type T = R}
    ): TableExpr{type T = R} =
       a.in { (t) =>
        t[T0,T1,T2](0,1,2) { case (c0,c1,c2) =>
          body(c0,c1,c2)
        }
      }

  }
  implicit class ColumnSyntax4[T0,T1,T2,T3](a: TableExpr {
    type T = ConstMarker[(T0,T1,T2,T3)]
  }) {
    def columns[R](
        body: (
          DelayedIdent[T0],
          DelayedIdent[T1],
          DelayedIdent[T2],
          DelayedIdent[T3],
          ) => TableExpr{type T = R}
    ): TableExpr{type T = R} =
       a.in { (t) =>
        t[T0,T1,T2,T3](0,1,2,3) { case (c0,c1,c2,c3) =>
          body(c0,c1,c2,c3)
        }
      }

  }
  implicit class ColumnSyntax5[T0,T1,T2,T3,T4](a: TableExpr {
    type T = ConstMarker[(T0,T1,T2,T3,T4)]
  }) {
    def columns[R](
        body: (
          DelayedIdent[T0],
          DelayedIdent[T1],
          DelayedIdent[T2],
          DelayedIdent[T3],
          DelayedIdent[T4],
          ) => TableExpr{type T = R}
    ) : TableExpr{type T = R} =
       a.in { (t) =>
        t[T0,T1,T2,T3,T4](0,1,2,3,4) { case (c0,c1,c2,c3,c4) =>
          body(c0,c1,c2,c3,c4)
        }
      }

  }
  case class curryColumnsTuple[T0<:Tuple]( a: TableExpr {
    type T = ra3.lang.ReturnValueTuple[T0]
  }){
    inline def apply[R](
        inline body: a.T#MM => TableExpr{type T = R}
    ) : TableExpr{type T = R}=
       a.in { (t) =>
        t.tuple[T0].apply { case (c0) =>
          body(c0)
        }
      }
  }
  extension [T0<:Tuple]( a: TableExpr {
    type T = ra3.lang.ReturnValueTuple[T0]
  }) {
    // inline def t0 : T0 = ???
    // inline def mm : a.T#MM = ???
    inline def columnsTuple = curryColumnsTuple[T0](a)
  }

  // implicit class ReturnValueSyntaxStar[T0<:Tuple](a: TableExpr {
  //   type T = ra3.lang.ReturnValue.ReturnValueTuple[T0]
  // }) {
  //   def columns[R](
  //       body: Tuple.Map[T0,ra3.lang.DelayedIdent] => TableExpr{type T = R}
  //   ) : TableExpr{type T = R}=
  //      a.in { (t) =>
  //       t.tuple[T0] { case (c0) =>
  //         body(c0)
  //       }
  //     }

  // }
  implicit class ReturnValueSyntax1[T0](a: TableExpr {
    type T = ra3.lang.ReturnValue1[T0]
  }) {
    def columns[R](
        body: DelayedIdent[T0] => TableExpr{type T = R}
    ) : TableExpr{type T = R}=
       a.in { (t) =>
        t[T0](0) { case (c0) =>
          body(c0)
        }
      }

  }
  implicit class ReturnValueSyntax2[T0, T1](a: TableExpr {
    type T = ra3.lang.ReturnValue2[T0, T1]
  }) {
    def columns[R](
        body: (
            DelayedIdent[T0],
            DelayedIdent[T1]
        ) => TableExpr{type T = R}
    ) : TableExpr{type T = R} = 
      a.in { t =>
        t[T0, T1](0, 1) { case (c0, c1) =>
          body(c0, c1)
        }
      }
    

  }
  implicit class ReturnValueSyntax3[T0, T1, T2](a: TableExpr {
    type T = ra3.lang.ReturnValue3[T0, T1,T2]
  }) {
    def columns[R](
        body: (
            DelayedIdent[T0],
            DelayedIdent[T1],
            DelayedIdent[T2],
        ) => TableExpr{type T = R}
    ) : TableExpr{type T = R}= 
      a.in { t =>
        t[T0, T1,T2](0, 1,2) { case (c0, c1, c2) =>
          body(c0, c1,c2)
        }
      }
    

  }
  implicit class ReturnValueSyntax4[T0, T1, T2, T3](a: TableExpr {
    type T = ra3.lang.ReturnValue4[T0, T1,T2, T3]
  }) {
    def columns[R](
        body: (
            DelayedIdent[T0],
            DelayedIdent[T1],
            DelayedIdent[T2],
            DelayedIdent[T3],
        ) => TableExpr{type T = R}
    ) : TableExpr{type T = R}= 
      a.in { t =>
        t[T0, T1,T2,T3](0, 1,2,3) { case (c0, c1, c2, c3) =>
          body(c0, c1,c2, c3)
        }
      }
    

  }
  implicit class ReturnValueSyntax5[T0, T1, T2, T3,T4](a: TableExpr {
    type T = ra3.lang.ReturnValue5[T0, T1,T2, T3,T4]
  }) {
    def columns[R](
        body: (
            DelayedIdent[T0],
            DelayedIdent[T1],
            DelayedIdent[T2],
            DelayedIdent[T3],
            DelayedIdent[T4],
        ) => TableExpr{type T = R}
    ) : TableExpr{type T = R}= 
      a.in { t =>
        t[T0, T1,T2,T3,T4](0, 1,2,3,4) { case (c0, c1, c2, c3,c4) =>
          body(c0, c1,c2, c3,c4)
        }
      }
    

  }

  import scala.language.implicitConversions
  implicit def convertJoinBuilder[J,R](j: ra3.lang.JoinBuilderSyntax[J,R]): TableExpr =
    j.done

  implicit val codec: JsonValueCodec[TableExpr] = ???
    // JsonCodecMaker.make(CodecMakerConfig.withAllowRecursiveTypes(true))

  type ConstMarker[A]

  def const[T1](table: ra3.Table): TableExpr { type T = ConstMarker[T1] } =
    TableExpr.Const(table).asInstanceOf[TableExpr { type T = ConstMarker[T1] }]

  case class Const(table: ra3.Table) extends TableExpr {

    def evalWith(env: Map[Key, TableValue])(implicit
        tsc: TaskSystemComponents
    ) =
      IO.pure(TableValue(table))

    def tags: Set[KeyTag] = Set.empty

    override def replace(i: Map[KeyTag, Int]) = this
  }
  case class Ident(private[ra3] key: Key) extends TableExpr { self =>


    private[ra3] def evalWith(env: Map[Key, TableValue])(implicit
        tsc: TaskSystemComponents
    ) =
      IO.pure(env(key))

    private[ra3] def tags: Set[KeyTag] = key match {
      case TagKey(s) => Set(s)
      case _         => Set.empty
    }

    private[ra3] override def replace(i: Map[KeyTag, Int]) : Ident = key match {
      case TagKey(s) => Ident(IntKey(i(s)))
      case _         => this
    }

    def tap(tag: String, size: Int = 50) = Tap(this, size, tag)

    def reduce[A](
        prg: ra3.lang.Query[A]
    ) : TableExpr{type T = ReturnValue[A]} =
      ra3.tablelang.TableExpr.ReduceTable(
        arg0 = this,
        groupwise = prg
      )
    def partialReduce[A](
        prg: ra3.lang.Query[A]
    ) : TableExpr{type T = ReturnValue[A]}=
      ra3.tablelang.TableExpr.FullTablePartialReduce(
        arg0 = this,
        groupwise = prg
      )

    def query[A](prg: ra3.lang.Query[A]) : TableExpr{type T = ReturnValue[A]}=
      ra3.tablelang.TableExpr.SimpleQuery(this, prg)

   
    def count[A](prg: ra3.lang.Query[A]) =
      ra3.tablelang.TableExpr
        .SimpleQueryCount(this, prg)

    // @scala.annotation.nowarn
    def apply[T0: NotNothing](
        n1: String
    ) = {
      Expr.DelayedIdent(ra3.lang.Delayed(this.key, Left(n1)))

    }
 

    case class curry1[T0: NotNothing](
      d1: DelayedIdent[T0],
    ) {
      def apply[R](body:(
            DelayedIdent[T0],
        ) => TableExpr{type T = R}) : TableExpr{type T = R} = body(d1)
    }
    def apply[T0: NotNothing](
        n1: Int,
    ) = {
      val d1 = Expr.DelayedIdent[T0](ra3.lang.Delayed(this.key, Right(n1)))
      curry1(d1)
    }
    case class curry2[T0: NotNothing, T1: NotNothing](
      d1: DelayedIdent[T0],
      d2: DelayedIdent[T1],
    ) {
      def apply[R](body:(
            DelayedIdent[T0],
            DelayedIdent[T1],
        ) => TableExpr{type T = R}) : TableExpr{type T = R} = body(d1,d2)
    }
    def apply[T0: NotNothing, T1: NotNothing](
        n1: Int,
        n2: Int,
    ) = {
      val d1 = Expr.DelayedIdent[T0](ra3.lang.Delayed(this.key, Right(n1)))
      val d2 = Expr.DelayedIdent[T1](ra3.lang.Delayed(this.key, Right(n2)))
      curry2(d1, d2)
    }
    case class curry3[T0: NotNothing, T1: NotNothing, T2: NotNothing](
      d1: DelayedIdent[T0],
      d2: DelayedIdent[T1],
      d3: DelayedIdent[T2],
    ) {
      def apply[R](body:(
            DelayedIdent[T0],
            DelayedIdent[T1],
            DelayedIdent[T2],
        ) => TableExpr{type T = R}) : TableExpr{type T = R} = body(d1,d2,d3)
    }
    def apply[T0: NotNothing, T1: NotNothing, T2: NotNothing](
        n1: Int,
        n2: Int,
        n3: Int,
    ) = {
      val d1 = Expr.DelayedIdent[T0](ra3.lang.Delayed(this.key, Right(n1)))
      val d2 = Expr.DelayedIdent[T1](ra3.lang.Delayed(this.key, Right(n2)))
      val d3 = Expr.DelayedIdent[T2](ra3.lang.Delayed(this.key, Right(n3)))
      curry3(d1, d2, d3)
    }

    case class curry4[T0: NotNothing, T1: NotNothing, T2: NotNothing, T3: NotNothing](
      d1: DelayedIdent[T0],
      d2: DelayedIdent[T1],
      d3: DelayedIdent[T2],
      d4: DelayedIdent[T3],
    ) {
      def apply[R](body:(
            DelayedIdent[T0],
            DelayedIdent[T1],
            DelayedIdent[T2],
            DelayedIdent[T3]
        ) => TableExpr{type T = R}) : TableExpr{type T = R} = body(d1,d2,d3,d4)
    }
    def apply[T0: NotNothing, T1: NotNothing, T2: NotNothing, T3: NotNothing](
        n1: Int,
        n2: Int,
        n3: Int,
        n4: Int
    ) = {
      val d1 = Expr.DelayedIdent[T0](ra3.lang.Delayed(this.key, Right(n1)))
      val d2 = Expr.DelayedIdent[T1](ra3.lang.Delayed(this.key, Right(n2)))
      val d3 = Expr.DelayedIdent[T2](ra3.lang.Delayed(this.key, Right(n3)))
      val d4 = Expr.DelayedIdent[T3](ra3.lang.Delayed(this.key, Right(n4)))
      curry4(d1, d2, d3, d4)
    }

    case class curry5[T0: NotNothing, T1: NotNothing, T2: NotNothing, T3: NotNothing, T4: NotNothing](
      d1: DelayedIdent[T0],
      d2: DelayedIdent[T1],
      d3: DelayedIdent[T2],
      d4: DelayedIdent[T3],
      d5: DelayedIdent[T4],
    ) {
      def apply[R](body:(
            DelayedIdent[T0],
            DelayedIdent[T1],
            DelayedIdent[T2],
            DelayedIdent[T3],
            DelayedIdent[T4],
        ) => TableExpr{type T = R}) : TableExpr{type T = R} = body(d1,d2,d3,d4,d5)
    }
    def apply[T0: NotNothing, T1: NotNothing, T2: NotNothing, T3: NotNothing, T4: NotNothing](
        n1: Int,
        n2: Int,
        n3: Int,
        n4: Int,
        n5: Int,
    ) = {
      val d1 = Expr.DelayedIdent[T0](ra3.lang.Delayed(this.key, Right(n1)))
      val d2 = Expr.DelayedIdent[T1](ra3.lang.Delayed(this.key, Right(n2)))
      val d3 = Expr.DelayedIdent[T2](ra3.lang.Delayed(this.key, Right(n3)))
      val d4 = Expr.DelayedIdent[T3](ra3.lang.Delayed(this.key, Right(n4)))
      val d5 = Expr.DelayedIdent[T4](ra3.lang.Delayed(this.key, Right(n5)))
      curry5(d1, d2, d3, d4, d5)
    }

    case class curryT[T0<:Tuple](
      d: Tuple.Map[T0,ra3.lang.DelayedIdent]
    ) {
      
      inline def apply[R](body:(
            Tuple.Map[T0,ra3.lang.DelayedIdent]
        ) => TableExpr{type T = R}) : TableExpr{type T = R} = body(d)
    }
    inline def tuple[T<:Tuple] = {
      val size   = scala.compiletime.constValue[scala.Tuple.Size[T]]
      val list = 0 until size map {i =>
        Expr.DelayedIdent(ra3.lang.Delayed(this.key, Right(i)))
      }
      val tup = scala.Tuple.fromArray(list.toArray).asInstanceOf[Tuple.Map[T,ra3.lang.DelayedIdent]]

      curryT(tup)
      

    }

   

  }

  case class Local(name: Key, assigned: TableExpr, body: TableExpr)
      extends TableExpr {
    self =>
    type T = body.T

    val tags: Set[KeyTag] = name match {
      case TagKey(s) => Set(s) ++ assigned.tags ++ body.tags
      case _         => assigned.tags ++ body.tags
    }

    def replace(i: Map[KeyTag, Int]): TableExpr = {
      val replacedName = name match {
        case TagKey(t) => IntKey(i(t))
        case n         => n
      }
      Local(replacedName, assigned.replace(i), body.replace(i))
    }
    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      assigned.evalWith(env).flatMap { assignedValue =>
        body.evalWith(env + (name -> assignedValue))
      }
    }

  }

  case class SimpleQuery[K,A<:ReturnValue[K]](
      arg0: TableExpr.Ident,
      elementwise: ra3.lang.Expr[A]
  ) extends TableExpr {

    type T = A

    val tags: Set[KeyTag] = arg0.tags
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      SimpleQuery(
        arg0.replace(i).asInstanceOf[Ident],
        elementwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          scribe.info(
            f"Will do simpl query on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments)"
          )
          ra3.SimpleQuery
            .simpleQuery(
              table,
              elementwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(arg0.key -> (table.uniqueId, table.colNames))
                  )
                )
            )
            .map(t => TableValue(t))

        }

    }
  }
  case class SimpleQueryCount[K,A<:ReturnValue[K]](
      arg0: TableExpr.Ident,
      elementwise: ra3.lang.Expr[A]
  ) extends TableExpr {

    type T = A

    val tags: Set[KeyTag] = arg0.tags
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      SimpleQueryCount(
        arg0.replace(i).asInstanceOf[Ident],
        elementwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          scribe.info(
            f"Will do simple query count on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments)"
          )
          ra3.SimpleQuery
            .simpleQueryCount(
              table,
              elementwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(arg0.key -> (table.uniqueId, table.colNames))
                  )
                )
            )
            .map(t => TableValue(t))

        }

    }
  }
  case class Tap(
      tracked val arg0: TableExpr.Ident,
      sampleSize: Int,
      tag: String
  ) extends TableExpr {

    type T = arg0.T

    val tags: Set[KeyTag] = arg0.tags
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      Tap(
        arg0.replace(i),
        sampleSize,
        tag
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          table.showSample(sampleSize).map { str =>
            scribe.info(
              f"Tapping $tag: \n $table \n $str"
            )
            TableValue(table)
          }

        }

    }
  }
  case class GroupThenReduce[K,A<:ReturnValue[K]](
      arg0: ra3.lang.Expr.DelayedIdent[?],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[?])],
      groupwise: ra3.lang.Expr[A],
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int
  ) extends TableExpr {

    type T = A

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      GroupThenReduce(
        arg0.replace(Map.empty, i),
        arg1.map(
          _.replace(Map.empty, i)
        ),
        groupwise.replaceTags(i),
        partitionBase,
        partitionLimit,
        maxSegmentsToBufferAtOnce
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0.tableIdent
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          val cols = (arg0 +: arg1).map(
            _.name.selection.left
              .map(s => table.colNames.indexOf(s))
              .fold(identity, identity)
          )
          scribe.info(
            f"Will do group by on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments) on ${cols.size} columns"
          )
          table
            .groupBy(
              cols = cols,
              partitionBase = partitionBase,
              partitionLimit = partitionLimit,
              maxSegmentsToBufferAtOnce = maxSegmentsToBufferAtOnce
            )
            .flatMap { groupedTable =>
              scribe.info(
                f"Will do reduction on ${groupedTable.uniqueId} (${groupedTable.partitions.size} partitions) "
              )
              val program = groupwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(
                      arg0.name.table -> (groupedTable.uniqueId, groupedTable.colNames)
                    )
                  )
                )
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class GroupThenCount[K,A<:ReturnValue[K]](
      arg0: ra3.lang.Expr.DelayedIdent[?],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[?])],
      groupwise: ra3.lang.Expr[A],
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int
  ) extends TableExpr {

    type T = ra3.DI64

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      GroupThenCount(
        arg0.replace(Map.empty, i),
        arg1.map(
          _.replace(Map.empty, i)
        ),
        groupwise.replaceTags(i),
        partitionBase,
        partitionLimit,
        maxSegmentsToBufferAtOnce
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0.tableIdent
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          val cols = (arg0 +: arg1).map(
            _.name.selection.left
              .map(s => table.colNames.indexOf(s))
              .fold(identity, identity)
          )
          scribe.info(
            f"Will do group by on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments) on ${cols.size} columns"
          )
          table
            .groupBy(
              cols = cols,
              partitionBase = partitionBase,
              partitionLimit = partitionLimit,
              maxSegmentsToBufferAtOnce = maxSegmentsToBufferAtOnce
            )
            .flatMap { groupedTable =>
              scribe.info(
                f"Will do reduction on ${groupedTable.uniqueId} (${groupedTable.partitions.size} partitions) "
              )
              val program = groupwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(
                      arg0.name.table -> (groupedTable.uniqueId, groupedTable.colNames)
                    )
                  )
                )
                
              GroupedTable.countGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class GroupPartialThenReduce[K,A<:ReturnValue[K]](
      arg0: ra3.lang.Expr.DelayedIdent[?],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[?])],
      groupwise: ra3.lang.Expr[A]
  ) extends TableExpr {

    type T = A

    assert(arg1.forall(_.name.table == arg0.name.table))

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_.tableIdent.tags)
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      GroupPartialThenReduce(
        arg0.replace(Map.empty, i),
        arg1.map(
          _.replace(Map.empty, i)
        ),
        groupwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0.tableIdent
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          val cols = (arg0 +: arg1).map(
            _.name.selection.left
              .map(s => table.colNames.indexOf(s))
              .fold(identity, identity)
          )
          scribe.info(
            f"Will do partial group by segments  on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments) on ${cols.size} columns"
          )
          table
            .groupBySegments(
              cols = cols
            )
            .flatMap { groupedTable =>
              scribe.info(
                f"Will do partial reduction on ${groupedTable.uniqueId} (${groupedTable.partitions.size} partitions) "
              )
              val program = groupwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(
                      arg0.name.table -> (groupedTable.uniqueId, groupedTable.colNames)
                    )
                  )
                )
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class FullTablePartialReduce[K,A<:ReturnValue[K]](
      arg0: TableExpr.Ident,
      groupwise: ra3.lang.Expr[A]
  ) extends TableExpr {

    type T = A

    val tags: Set[KeyTag] = arg0.tags
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      FullTablePartialReduce(
        arg0.replace(i),
        groupwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          ra3.ReduceTable
            .formSingleGroupAsOnePartitionPerSegment(table)
            .flatMap { groupedTable =>
              scribe.info(
                f"Will do partial reduction on ${groupedTable.uniqueId} (${groupedTable.partitions.size} partitions) "
              )

              val program = groupwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(
                      arg0.key -> (groupedTable.uniqueId, groupedTable.colNames)
                    )
                  )
                )
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class ReduceTable[K,A<:ReturnValue[K]](
      arg0: TableExpr.Ident,
      groupwise: ra3.lang.Expr[A]
  ) extends TableExpr {

    type T = A

    val tags: Set[KeyTag] = arg0.tags
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      ReduceTable(
        arg0.replace(i),
        groupwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          scribe.info(
            f"Will do full table reduction at once  on ${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments) "
          )

          ra3.ReduceTable
            .formSingleGroup(table)
            .flatMap { groupedTable =>
              val program = groupwise
                .replaceDelayed(
                  DelayedTableSchema(
                    Map(
                      arg0.key -> (groupedTable.uniqueId, groupedTable.colNames)
                    )
                  )
                )
              GroupedTable.reduceGroups(groupedTable, program)
            }
            .map(t => TableValue(t))

        }

    }
  }
  case class Join[J,K,R<:ReturnValue[K]](
      arg0: ra3.lang.Expr.DelayedIdent[J],
      arg1: Seq[(ra3.lang.Expr.DelayedIdent[J], String, ra3.tablelang.Key)],
      partitionBase: Int,
      partitionLimit: Int,
      maxSegmentsToBufferAtOnce: Int,
      elementwise: ra3.lang.Expr[R]
  ) extends TableExpr {

    val tags: Set[KeyTag] =
      arg0.tableIdent.tags ++ arg1.flatMap(_._1.tableIdent.tags)
    def replace(i: Map[KeyTag, Int]): TableExpr = {

      Join(
        arg0.replace(Map.empty, i),
        arg1.map { case (columnAndTable, how, against) =>
          (
            columnAndTable
              .replace(Map.empty, i),
            how,
            against
          )
        },
        partitionBase,
        partitionLimit,
        maxSegmentsToBufferAtOnce,
        elementwise.replaceTags(i)
      )
    }

    def evalWith(
        env: Map[Key, TableValue]
    )(implicit tsc: TaskSystemComponents) = {
      arg0.tableIdent
        .evalWith(env)
        .flatMap { case TableValue(table) =>
          IO.parSequenceN(32)(arg1.map { case (a, b, c) =>
            a.tableIdent.evalWith(env).map(x => (x.v, a, b, c))
          }).flatMap { arg1Tables =>
            val program = elementwise
              .replaceDelayed(
                DelayedTableSchema(
                  Map(
                    arg0.name.table -> ((table.uniqueId, table.colNames))
                  ) ++ (arg1Tables.map(_._1) zip arg1.map(_._1)).map {
                    case (table, argxDelayed) =>
                      (argxDelayed.name.table, (table.uniqueId, table.colNames))
                  }
                )
              )

            val firstColIdx = arg0.name.selection.left
              .map(s => table.colNames.indexOf(s))
              .fold(identity, identity)

            def tableStrings = (List(table) ++ arg1Tables.map(_._1)).map {
              table =>
                f"${table.uniqueId} (${table.numRows}%,d x ${table.numCols} in ${table.segmentation.size}%,d segments)  "
            }

            scribe.info(
              f"Will join the following tables: ${tableStrings.mkString(" x ")} "
            )

            ra3.Equijoin
              .equijoinPlanner(
                table,
                firstColIdx,
                arg1Tables.map {
                  case (table, delayedCol, how, delayedAgainst) =>
                    val colIdx: Int = delayedCol.name.selection.left
                      .map(s => table.colNames.indexOf(s))
                      .fold(identity, identity)
                    val againstTable: Int =
                      if (arg0.name.table == delayedAgainst) 0
                      else {
                        val c = arg1.zipWithIndex
                          .find(_._1._1.name.table == delayedAgainst)
                          .get
                          ._2
                        c + 1
                      }

                    (table, colIdx, how, againstTable)
                },
                partitionBase,
                partitionLimit,
                maxSegmentsToBufferAtOnce,
                program
              )
              .map(TableValue(_))
          }

        }

    }
  }

  // implicit class SyntaxIdent[A: NotNothing, B: NotNothing](a: TableExpr.Ident {
  //   type T = ra3.lang.ReturnValue2[A, B]
  // }) {
  //   def in[R](
  //       body: (
  //           Expr.DelayedIdent {
  //             type T = A; type R = ra3.lang.ReturnValue2[A, B]
  //           },
  //           Expr.DelayedIdent {
  //             type T = B; type R = ra3.lang.ReturnValue2[A, B]
  //           }
  //       ) => TableExpr { type T = R }
  //   ): TableExpr { type T = R } = {
  //     val aa = Expr
  //       .DelayedIdent(ra3.lang.Delayed(a.key, Right(0)))
  //       .asInstanceOf[
  //         Expr.DelayedIdent { type T = A; type R = ra3.lang.ReturnValue2[A, B] }
  //       ]
  //     val bb = Expr
  //       .DelayedIdent(ra3.lang.Delayed(a.key, Right(1)))
  //       .asInstanceOf[
  //         Expr.DelayedIdent { type T = B; type R = ra3.lang.ReturnValue2[A, B] }
  //       ]

  //     body(aa, bb)

  //   }
  // }

}

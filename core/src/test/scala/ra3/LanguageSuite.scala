package ra3
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.unsafe.implicits.global
import lang.{global => gl, _}

class LanguageSuite extends munit.FunSuite with WithTempTaskSystem {
  test("basic") {
    withTempTaskSystem { implicit ts =>
      def eval(expr: Expr, map: Map[Key, Value[_]]) =
        ra3.lang.evaluate(expr, map).unsafeRunSync()
      val e: Expr =
        lang.global[Int](ColumnKey("hole", 0))((hole: IntExpr) =>
          local(Expr.LitNum(1))((g1: IntExpr) => {
            (hole: Expr.SyntaxIntExpr).+(g1)

          }
          // lang.local(Expr.LitNum(99 + 1))(g100 => (hole + g1 + g100 + g100 - Expr.LitNum(2)).asString)
          )
        )
      val e2: Expr =
        lang.global[Int](ColumnKey("hole", 0))(hole =>
          let(Expr.LitNum(1))(g1 =>
            let(Expr.LitNum(99 + 1))(g100 =>
              (hole ++ g1 ++ g100 ++ g100 - Expr.LitNum(2)).asString
            )
          )
        )

      assert(
        evaluate(let(Expr.LitNum(1)) { e1 =>
          star :: (e1 ++ e1).as("b") :: e1.as("a").list
        }).unsafeRunSync().v == List(
          ra3.lang.Star,
          NamedConstantI32(2, "b"),
          NamedConstantI32(1, "a")
        )
      )

      gl[ra3.DI32](ColumnKey("hole", 0))(buffer =>
        select(buffer as "boo", buffer as "boo2", buffer)
          where (buffer <= 0)
      ).replaceTags(Map.empty)

      val e3 = readFromString[Expr](writeToString(e.replaceTags(Map.empty)))
      val e4 = readFromString[Expr](writeToString(e2.replaceTags(Map.empty)))
      assert(
        writeToString(e.replaceTags(Map.empty)) == writeToString(
          e2.replaceTags(Map.empty)
        )
      )
      println(eval(e, Map(ColumnKey("hole", 0) -> Value.Const(1))).v)
      assert(eval(e, Map(ColumnKey("hole", 0) -> Value.Const(1))).v == "200")
      assert(eval(e3, Map(ColumnKey("hole", 0) -> Value.Const(1))).v == "200")
      assert(
        eval(
          e.replaceTags(Map.empty),
          Map(ColumnKey("hole", 0) -> Value.Const(1))
        ).v == "200"
      )
      assert(
        eval(
          e2.replaceTags(Map.empty),
          Map(ColumnKey("hole", 0) -> Value.Const(1))
        ).v == "200"
      )
      assert(
        eval(
          e3.replaceTags(Map.empty),
          Map(ColumnKey("hole", 0) -> Value.Const(1))
        ).v == "200"
      )
      assert(eval(e3, Map(ColumnKey("hole", 0) -> Value.Const(1))).v == "200")
      assert(eval(e4, Map(ColumnKey("hole", 0) -> Value.Const(1))).v == "200")

    }
  }
  // test("Buffer") {
  //   import lang._
  //   val e: lang.Expr =
  //     lang.global[BufferInt](ColumnKey("hole",0))(hole => hole.abs)

  //   println(e)
  //   println(writeToString(e))
  //   val e2 = readFromString[Expr](writeToString(e))
  //   assert(
  //     evaluate(
  //       e2,
  //       Map(ColumnKey("hole",0) -> Value.Const(BufferIntConstant(-3, 3)))
  //     ).v == BufferIntConstant(3, 3)
  //   )

  // }
}

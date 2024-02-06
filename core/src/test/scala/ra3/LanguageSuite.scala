package ra3
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.unsafe.implicits.global
import lang.{global => gl, _}

class LanguageSuite extends munit.FunSuite with WithTempTaskSystem {
  test("basic") {
    withTempTaskSystem{ implicit ts =>

      def eval(expr: Expr, map: Map[Key, Value[_]]) =
    ra3.lang.evaluate(expr,map).unsafeRunSync()
    val e: Expr =
      lang.global[Int](ColumnKey("hole", 0))(hole =>
        lang.local(1)(g1 =>
          lang.local(99 + 1)(g100 => (hole + g1 + g100 + g100 - 2).asString)
        )
      )
    val e2: Expr =
      lang.global[Int](ColumnKey("hole", 0))(hole =>
        lang.let(1)(g1 =>
          lang.let(99 + 1)(g100 => (hole + g1 + g100 + g100 - 2).asString)
        )
      )

    assert(evaluate(let(1) { e1 =>
      lang.star :: (e1 + e1).as("b") :: e1.as("a").list
    }).unsafeRunSync().v == List(ra3.lang.Star, NamedConstantI32(2, "b"), NamedConstantI32(1, "a")))

    val s1 = gl[ra3.lang.DI32](ColumnKey("hole", 0))(buffer =>
        select(buffer as "boo", buffer as "boo2", buffer)
          where (buffer <= 0 )
      ).replaceTags()

    println(
      s1
    )
    // s1 match {
    //   case LitNum(s) => 
    //   case BuiltInOpStar(args, op) =>
    //   case BuiltInOp3(arg0, arg1, arg2, op) =>
    //   case BuiltInOp1(arg0, op) =>
    //   case Ident(name) =>
    //   case BuiltInOp2(arg0, arg1, ra3.lang.ops.MkSelect) =>
    //   case BuiltInOp2(arg0, arg1, op) =>
    //   case Expr.Star =>
    //   case Local(name, assigned, body) =>
    //   case LitStr(s) =>
    // }

    println(e)
    println(e2)

    println(writeToString(e))
    println(writeToString(e2))
    println(writeToString(e.replaceTags()))
    println(writeToString(e2.replaceTags()))
    val e3 = readFromString[Expr](writeToString(e.replaceTags()))
    val e4 = readFromString[Expr](writeToString(e2.replaceTags()))
    assert(writeToString(e.replaceTags()) == writeToString(e2.replaceTags()))
    println(eval(e, Map(ColumnKey("hole", 0) -> Value.Const(1))).v)
    assert(eval(e, Map(ColumnKey("hole", 0) -> Value.Const(1))).v == "200")
    assert(eval(e3, Map(ColumnKey("hole", 0) -> Value.Const(1))).v == "200")
    assert(
      eval(
        e.replaceTags(),
        Map(ColumnKey("hole", 0) -> Value.Const(1))
      ).v == "200"
    )
    assert(
      eval(
        e2.replaceTags(),
        Map(ColumnKey("hole", 0) -> Value.Const(1))
      ).v == "200"
    )
    assert(
      eval(
        e3.replaceTags(),
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

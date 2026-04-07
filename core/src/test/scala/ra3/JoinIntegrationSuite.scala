package ra3

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.Duration

class JoinIntegrationSuite extends munit.FunSuite with WithTempTaskSystem {
  override val munitTimeout = Duration(180, "s")

  test("inner join on string key") {
    withTempTaskSystem { implicit tsc =>
      val csvLeft =
        """id,name
          |1,alice
          |2,bob
          |3,carol""".stripMargin

      val csvRight =
        """id,score
          |2,90.0
          |3,80.0
          |4,70.0""".stripMargin

      val left = csvStringToHeterogeneousTable(
        "left",
        csvLeft,
        List(
          (0, ColumnTag.I32, None, None),
          (1, ColumnTag.StringTag, None, None)
        ),
        100
      )
      val right = csvStringToHeterogeneousTable(
        "right",
        csvRight,
        List(
          (0, ColumnTag.I32, None, None),
          (1, ColumnTag.F64, None, None)
        ),
        100
      )

      val result = left
        .as[("id", "name"), (I32Var, StrVar)]
        .schema { l =>
          right
            .as[("id", "score"), (I32Var, F64Var)]
            .schema { r =>
              l.id
                .inner(r.id)
                .withPartitionBase(3)
                .withPartitionLimit(0)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(
                  l.name.as("name") :*
                    r.score.as("score")
                )
            }
        }
        .evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()
        .map(v => (v.name.toString, v.score))
        .toSet

      assertEquals(result, Set(("bob", 90.0), ("carol", 80.0)))
    }
  }

  test("outer join with ifelse to coalesce keys") {
    withTempTaskSystem { implicit tsc =>
      val dataA = Seq(("alice", 10.0), ("bob", 20.0))
      val dataB = Seq(("bob", 30.0), ("carol", 40.0))

      val tableA = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](dataA.iterator, 256),
          uniqueId = "joinA",
          minimumSegmentSize = 10,
          maximumSegmentSize = 100
        )
        .unsafeRunSync()
        .rename[("name", "valueA")]

      val tableB = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](dataB.iterator, 256),
          uniqueId = "joinB",
          minimumSegmentSize = 10,
          maximumSegmentSize = 100
        )
        .unsafeRunSync()
        .rename[("name", "valueB")]

      val result = tableA.schema { a =>
        tableB.schema { b =>
          a.name
            .outer(b.name)
            .select(
              a.name.isMissing
                .ifelse(b.name, a.name)
                .as("name") :*
                a.valueA.as("valueA") :*
                b.valueB.as("valueB")
            )
        }
      }.evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()
        .map(v =>
          (v.name.toString, f"${v.valueA}%.1f", f"${v.valueB}%.1f")
        )
        .toSet

      assertEquals(
        result,
        Set(
          ("alice", "10.0", "NaN"),
          ("bob", "20.0", "30.0"),
          ("carol", "NaN", "40.0")
        )
      )
    }
  }

  test("inner join with where filter") {
    withTempTaskSystem { implicit tsc =>
      val csvA =
        """k,a
          |1,10
          |2,20
          |3,30
          |1,40""".stripMargin

      val csvB =
        """k,b
          |1,100
          |2,200
          |4,400""".stripMargin

      val tableA = csvStringToTable("jA", csvA, 2, 100)
      val tableB = csvStringToTable("jB", csvB, 2, 100)

      val result = tableA
        .as[("k", "a"), (I32Var, I32Var)]
        .schema { a =>
          tableB
            .as[("k", "b"), (I32Var, I32Var)]
            .schema { b =>
              a.k
                .inner(b.k)
                .withPartitionBase(3)
                .withPartitionLimit(0)
                .withMaxSegmentsBufferingAtOnce(2)
                .select(
                  (a.k.as("k") :* a.a.as("a") :* b.b.as("b"))
                    .where(a.a >= 20)
                )
            }
        }
        .evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()
        .map(v => (v.k, v.a, v.b))
        .toSet

      // k=2,a=20,b=200 matches; k=1,a=40,b=100 matches; k=1,a=10 filtered out
      assertEquals(
        result,
        Set((2, 20, 200), (1, 40, 100))
      )
    }
  }

  test("self-join") {
    withTempTaskSystem { implicit tsc =>
      val data = Seq(("a", 1), ("b", 2), ("a", 3))

      val table = ra3
        .importFromStream(
          stream = fs2.Stream.fromIterator[IO](data.iterator, 256),
          uniqueId = "selfj",
          minimumSegmentSize = 10,
          maximumSegmentSize = 100
        )
        .unsafeRunSync()
        .rename[("key", "v")]

      val result = table.schema { t1 =>
        table.schema { t2 =>
          t1.key
            .inner(t2.key)
            .withPartitionBase(3)
            .withPartitionLimit(0)
            .withMaxSegmentsBufferingAtOnce(2)
            .select(
              t1.key.as("key") :*
                t1.v.as("v1") :*
                t2.v.as("v2")
            )
        }
      }.evaluateToStream
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()
        .map(v => (v.key.toString, v.v1, v.v2))
        .toSet

      // a joins with a: (a,1,1),(a,1,3),(a,3,1),(a,3,3); b with b: (b,2,2)
      assertEquals(
        result,
        Set(
          ("a", 1, 1),
          ("a", 1, 3),
          ("a", 3, 1),
          ("a", 3, 3),
          ("b", 2, 2)
        )
      )
    }
  }
}

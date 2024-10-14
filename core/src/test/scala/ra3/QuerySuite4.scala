package ra3

import scala.util.Random
import java.io.File
import tasks._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration.Duration
import java.time.Instant

class QuerySuite4 extends munit.FunSuite with WithTempTaskSystem {
  override val munitTimeout = Duration(180, "s")
  test("test1") {

    def groupBy(
        customer: ra3.lang.Expr.DelayedIdent[StrVar],
        value: ra3.lang.Expr.DelayedIdent[F64Var]
    ) = {
      customer.groupBy
        .reducePartial(
          customer.first.unnamed :*
            value.sum.unnamed :*
            value.count.unnamed :*
            value.min.unnamed :*
            value.max.unnamed
        )
        .schema { case (customer, sum, count, min, max) =>
          _ =>
            customer.groupBy
              .reduceTotal(
                (customer.first `as` "customer") :*
                  ((sum.sum / count.sum) `as` "avg") :*
                  (min.min `as` "min") :*
                  (max.max `as` "max")
              )
        }
    }

    withTempTaskSystem { implicit tsc =>
      val data = runGenerate(30000)
      val prg = for {
        table <- parseTransactions(data)
        queryPrg <- IO.pure {
          for {
            byCustomerIn <- table.schema {
              (customerIn, customerOut, _, _, value, _) => _ =>
                groupBy(customerIn, value)
            }
            byCustomerOut <- table.schema {
              (_, customerOut, _, _, value, _) => _ =>
                groupBy(customerOut, value)
            }
            result <- byCustomerOut.schema {
              (customerOut, avgOutValue, _, _) => _ =>
                byCustomerIn.schema { (customerIn, avgInValue, _, _) => _ =>
                  customerIn
                    .outer(customerOut)
                    .select(
                      (customerIn as "cIn")
                        :* (customerOut as "cOut")
                        :* (
                          customerIn.isMissing
                            .ifelse(
                              customerOut,
                              customerIn
                            ) as "customer"
                        )
                        :* (avgInValue as "inAvg")
                        :* (avgOutValue as "outAvg")
                    )
                }
            }
          } yield result
        }

        _ <- IO { println(queryPrg.render) }

        eval <- queryPrg.evaluateToStream

      } yield eval

      val stream = prg.unsafeRunSync()
      val set = stream.compile.toList
        .unsafeRunSync()
        .toSet
        .map(v => (v._3.toString, f"${v._4}%.2f", f"${v._5}%.2f"))

      val expected = {
        val avgIn = data
          .groupBy(_._1)
          .toSeq
          .map(v => (v._1, v._2.map(_._5).sum / v._2.size))
          .toMap
        val avgOut = data
          .groupBy(_._2)
          .toSeq
          .map(v => (v._1, v._2.map(_._5).sum / v._2.size))
          .toMap
        (data.map(_._1) ++ data.map(_._2)).distinct.map { v =>
          val a = avgIn.get(v)
          val b = avgOut.get(v)
          (
            v,
            f"${a.getOrElse(Double.NaN)}%.2f",
            f"${b.getOrElse(Double.NaN)}%.2f"
          )

        }.toSet
      }

    println(expected.toSeq.sorted.take(100))
    assert(expected == set)

    }
  }

  def parseTransactions(
      s: Seq[(String, String, String, Int, Double, Instant)]
  )(implicit tsc: TaskSystemComponents) =
    for {

      table <- ra3.importFromStream(
        stream = fs2.Stream.fromIterator[IO](s.iterator, 256),
        uniqueId = "test123",
        minimumSegmentSize = 1000,
        maximumSegmentSize = 2000
      )

      sample <- table.table.showSample(nrows = 10)
      _ <- IO {
        println(sample)
      }
    } yield table

  /** Generate bogus data Each row is a transaction of some value between two
    * customers Each row consists of:
    *   - an id with cardinality in the millions, e.g. some customer id1
    *   - an id with cardinality in the millions, e.g. some customer id2
    *   - an id with cardinality in the hundred thousands e.g. some category id
    *   - an id with cardinality in the thousands e.g. some category id
    *   - a float value, e.g. a price
    *   - an instant
    *
    * The file is block gzipped
    */
  def runGenerate(size: Int) = {

    def makeRow() = {
      val millions = Random.alphanumeric.take(4).mkString
      val millions2 = Random.alphanumeric.take(4).mkString
      val hundredsOfThousands1 = Random.alphanumeric.take(3).mkString
      val thousands = Random.nextInt(5000)
      val float = Random.nextDouble()
      val instant = java.time.Instant
        .now()
        .plus(
          Random.nextLong(1000 * 60 * 60 * 24L),
          java.time.temporal.ChronoUnit.MILLIS
        )
      (millions, millions2, hundredsOfThousands1, thousands, float, instant)
    }

    Iterator
      .continually {
        makeRow()

      }
      .take(size)
      .toSeq

  }

}

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
          customer.first.as("customer") :*
            value.sum.as("sum") :*
            value.count.as("count") :*
            value.min.as("min") :*
            value.max.as("max")
        )
        .schema { case t =>
          t.customer.groupBy
            .reduceTotal(
              (t.customer.first `as` "customer") :*
                ((t.sum.sum / t.count.sum) `as` "avg") :*
                (t.min.min `as` "min") :*
                (t.max.max `as` "max")
            )
        }
    }

    withTempTaskSystem { implicit tsc =>
      val data = runGenerate(30000)
      val prg = for {
        table <- parseTransactions(data)
        queryPrg <- IO.pure {
          for {
            byCustomerIn <- table.schema { t =>
              groupBy(t.customerIn, t.value)
            }
            byCustomerOut <- table.schema { t =>
              groupBy(t.customerOut, t.value)
            }
            result <-
              byCustomerIn.customer
                .outer(byCustomerOut.customer)
                .select(
                  (byCustomerIn.customer as "cIn")
                    :* (byCustomerOut.customer as "cOut")
                    :* (
                      byCustomerIn.customer.isMissing
                        .ifelse(
                          byCustomerOut.customer,
                          byCustomerIn.customer
                        ) `as` "customer"
                    )
                    :* (byCustomerIn.avg `as` "inAvg")
                    :* (byCustomerOut.avg `as` "outAvg")
                )

          } yield result
        }

        _ <- IO { println(queryPrg.render) }

        eval <- queryPrg.evaluateToStream

      } yield eval

      val stream = prg.unsafeRunSync()
      val set = stream.compile.toList
        .unsafeRunSync()
        .toSet
        .map(v => (v.customer.toString, f"${v.inAvg}%.2f", f"${v.outAvg}%.2f"))

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
      .rename[("customerIn", "customerOut", "cat", "catid", "value", "time")]

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

package ra3
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

extension [A](a: IO[A]) {
  inline def logElapsed(using f: sourcecode.FullName) = Elapsed.logElapsed(a)
}

case class Elapsed(s: Ref[IO, Map[sourcecode.FullName, (Long, Long)]])
object Elapsed {

  val globalElapsed: Elapsed = Elapsed(
    Ref[IO].of(Map[sourcecode.FullName, (Long, Long)]()).unsafeRunSync()
  )
  def logResult = globalElapsed.s.get.flatMap { map =>
    val txt =
      f"${"Method"}%-60s${"Count"}%10s${"total(s)"}%10s${"avg(s)"}%10s\n" + map.toSeq
        .sortBy(v => v._2._1)
        .reverse
        .map { case (fullName, (sum, count)) =>
          f"${fullName.value}%-60s${count}%10d${sum.toDouble * 1e-9}%10.2g${sum.toDouble / count * 1e-9}%10.2g"
        }
        .mkString("\n")
    IO.delay(scribe.info(txt))
  }
  inline def logElapsed[A](inline a: IO[A])(using f: sourcecode.FullName) =
    for {
      d <- a.timed
      _ <- globalElapsed.s.getAndUpdate(st =>
        st.get(f) match {
          case None => st.updated(f, (d._1.toNanos, 1))
          case Some((sum, count)) =>
            st.updated(f, (d._1.toNanos + sum, count + 1))
        }
      )
    } yield d._2

}

package ra3
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

extension [A](a: IO[A]) {
  inline def countInF64(count: Int, bytes: Long) =
    IOMetricState.countInF64(a, count, bytes)
  inline def countInI64(count: Int, bytes: Long) =
    IOMetricState.countInI64(a, count, bytes)
  inline def countInI32(count: Int, bytes: Long) =
    IOMetricState.countInI32(a, count, bytes)
  inline def countInStr(count: Int, bytes: Long) =
    IOMetricState.countInStr(a, count, bytes)
  inline def countInInst(count: Int, bytes: Long) =
    IOMetricState.countInInst(a, count, bytes)
}

case class IOMetricCounts(
    bytesIn: Long,
    bytesOut: Long,
    elemIn: Long,
    elemOut: Long
) {
  def line = f"$elemIn%,20d${bytesIn / 1024 / 1024}%,20d"
}
object IOMetricCounts {
  def empty = IOMetricCounts(0, 0, 0, 0)
}
case class IOMetrics(
    i32: IOMetricCounts,
    i64: IOMetricCounts,
    f64: IOMetricCounts,
    str: IOMetricCounts,
    inst: IOMetricCounts
) {
  def updateInStr(count: Int, bytes: Long): IOMetrics =
    copy(str = str.copy(str.bytesIn + bytes, elemIn = str.elemIn + count))
  def updateInInst(count: Int, bytes: Long): IOMetrics =
    copy(inst = inst.copy(inst.bytesIn + bytes, elemIn = inst.elemIn + count))
  def updateInF64(count: Int, bytes: Long): IOMetrics =
    copy(f64 = f64.copy(f64.bytesIn + bytes, elemIn = f64.elemIn + count))
  def updateInI64(count: Int, bytes: Long): IOMetrics =
    copy(i64 = i64.copy(i64.bytesIn + bytes, elemIn = i64.elemIn + count))
  def updateInI32(count: Int, bytes: Long): IOMetrics =
    copy(i32 = i32.copy(i32.bytesIn + bytes, elemIn = i32.elemIn + count))
}
object IOMetrics {
  def empty = IOMetrics(
    IOMetricCounts.empty,
    IOMetricCounts.empty,
    IOMetricCounts.empty,
    IOMetricCounts.empty,
    IOMetricCounts.empty
  )
}
case class IOMetricState(r: Ref[IO, IOMetrics])
object IOMetricState {

  val global: IOMetricState = IOMetricState(
    Ref[IO].of(IOMetrics.empty).unsafeRunSync()
  )
  def logResult = global.r.get.flatMap { state =>
    val txt = f"T  ${"countIn"}%20s${"byteIn(MB)"}%20s\n" + List(
      "f64" + state.f64.line,
      "i64" + state.i64.line,
      "i32" + state.i32.line,
      "str" + state.str.line,
      "ins" + state.inst.line
    ).mkString("\n")
  IO.delay(scribe.info(txt))
  }
  def countInF64[A](a: IO[A], count: Int, bytes: Long) =
    for {
      _ <- global.r.getAndUpdate(st => st.updateInF64(count, bytes))
      a <- a
    } yield a
  def countInI64[A](a: IO[A], count: Int, bytes: Long) =
    for {
      _ <- global.r.getAndUpdate(st => st.updateInI64(count, bytes))
      a <- a
    } yield a
  def countInI32[A](a: IO[A], count: Int, bytes: Long) =
    for {
      _ <- global.r.getAndUpdate(st => st.updateInI32(count, bytes))
      a <- a
    } yield a
  def countInStr[A](a: IO[A], count: Int, bytes: Long) =
    for {
      _ <- global.r.getAndUpdate(st => st.updateInStr(count, bytes))
      a <- a
    } yield a
  def countInInst[A](a: IO[A], count: Int, bytes: Long) =
    for {
      _ <- global.r.getAndUpdate(st => st.updateInInst(count, bytes))
      a <- a
    } yield a

}

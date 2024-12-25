//> using scala 3.6.2
//> using option -experimental

object Generator {

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
  def runGenerate(size: Long, path: String): Unit = {
    import scala.util.Random
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

      (s"$millions\t$millions2\t$hundredsOfThousands1\t$thousands\t$float\t$instant\n")
    }

    Iterator
      .continually {
        makeRow()

      }
      .grouped(100_000)
      .take((size / 100_000 + 1).toInt)
      .zipWithIndex
      .foreach { case (group, idx) =>
        val fos1 = new java.util.zip.GZIPOutputStream(
          new java.io.FileOutputStream(path, true)
        )
        group.foreach { line =>
          fos1.write(line.getBytes("US-ASCII"))
        }
        println((idx + 1) * 100000)
        fos1.close
      }

  }

  def main(
      args: Array[String]
  ): Unit = {
    val size = args(0).toLong
    val path = args(1)
    println(s"Writing $size lines to $path")
    runGenerate(size, path)

  }

}

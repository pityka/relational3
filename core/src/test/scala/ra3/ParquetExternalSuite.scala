package ra3

import cats.effect.unsafe.implicits.global
import tasks.*

class ParquetExternalSuite extends munit.FunSuite with WithTempTaskSystem {

  val zstdB64 =
    "UEFSMRUAFSAVMiwVBhUAFQYVBgAAKLUv/SAQgQAABAAAAAQBAgABAAAAAgAAABUAFToVTCwVBhUAFQYVBgAAKLUv/SAd6QAABgAAAAIBAgACAQUAAABhcHBsZQYAAABjaGVycnkVABU8FUgsFQYVABUGFQYAACi1L/0gHt0AAKACAAAABgEA+D8ABEAAAAAAAAAMQAIAQMYFjBUCGUw1ABgNZHVja2RiX3NjaGVtYRUGABUCJQIYAWklIgAVDCUCGAFzJQAAFQolAhgBZAAWBhkcGTwmABwVAhkVABkYAWkVDBYGFkIWVCYIPBgEAgAAABgEAQAAABYCKAQCAAAAGAQBAAAAAAAAJgAcFQwZFQAZGAFzFQwWBhZcFm4mXDwYBmNoZXJyeRgFYXBwbGUWAigGY2hlcnJ5GAVhcHBsZQAAACYAHBUKGRUAGRgBZBUMFgYWXhZqJsoBPBgIAAAAAAAADEAYCAAAAAAAAPg/FgAoCAAAAAAAAAxAGAgAAAAAAAD4PwAAABaAjAcWBiYIACgGRHVja0RCAP8AAABQQVIx"

  val gzipB64 =
    "UEFSMRUAFSAVQiwVBhUAFQYVBgAAH4sIAAAAAAAA/2NhYGBgYWRiYATSTEAMAEX7VJgQAAAAFQAVOhVaLBUGFQAVBhUGAAAfiwgAAAAAAAD/Y2NgYGBiZAJiViArsaAgJ5UNyEjOSC0qqgQASCAtOx0AAAAVABU8FUwsFQYVABUGFQYAAB+LCAAAAAAAAP9jYmBgYGNkAIMf9hCaxQFC8zgAAGxcomAeAAAAFQIZTDUAGA1kdWNrZGJfc2NoZW1hFQYAFQIlAhgBaSUiABUMJQIYAXMlAAAVCiUCGAFkABYGGRwZPCYAHBUCGRUAGRgBaRUEFgYWQhZkJgg8GAQCAAAAGAQBAAAAFgIoBAIAAAAYBAEAAAAAAAAmABwVDBkVABkYAXMVBBYGFlwWfCZsPBgGY2hlcnJ5GAVhcHBsZRYCKAZjaGVycnkYBWFwcGxlAAAAJgAcFQoZFQAZGAFkFQQWBhZeFm4m6AE8GAgAAAAAAAAMQBgIAAAAAAAA+D8WACgIAAAAAAAADEAYCAAAAAAAAPg/AAAAFoCMBxYGJggAKAZEdWNrREIA/wAAAFBBUjE="

  private def readFixture(b64: String)(implicit
      tsc: ExtendedTaskSystemComponents
  ): Table = {
    val bytes = java.util.Base64.getDecoder.decode(b64)
    val tmp = java.io.File.createTempFile("ra3pqfix", ".parquet")
    java.nio.file.Files.write(tmp.toPath, bytes)
    val sf =
      SharedFile(tasks.util.Uri("file://" + tmp.getAbsolutePath)).unsafeRunSync()
    ra3.importParquet(sf, "ext", 1000).unsafeRunSync()
  }

  List(("zstd", zstdB64), ("gzip", gzipB64)).foreach { case (codec, b64) =>
    test(s"import external duckdb $codec dictionary-encoded parquet") {
      withTempTaskSystem { implicit tsc =>
        val t = readFixture(b64)
        val bt = t.bufferStream.compile.toList.unsafeRunSync().head
        assertEquals(t.colNames.toList, List("i", "s", "d"))
        assertEquals(
          bt.columns(1).toSeq.map(_.toString),
          List("apple", BufferString.missing.toString, "cherry")
        )
        assertEquals(
          bt.columns(2).toSeq.map(_.toString),
          List("1.5", "2.5", "3.5")
        )
        assert(!bt.columns(0).isMissing(0))
        assert(bt.columns(0).isMissing(2))
      }
    }
  }
}

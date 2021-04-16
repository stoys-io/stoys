package io.stoys.spark.test

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.jdk.CollectionConverters._

class SparkExampleBaseTest extends SparkExampleBase {
  import SparkExampleBaseTest._

  test("writeValueAsJsonFile") {
    val records = Seq(Record("foo", 42))
    val recordsJsonPath = writeValueAsJsonTmpFile("records.json", records)
    assert(recordsJsonPath.toFile.exists())
    val recordsJsonContent = Files.readAllLines(recordsJsonPath, StandardCharsets.UTF_8).asScala.mkString("\n")
    assert(recordsJsonContent ===
        """
          |[ {
          |  "s" : "foo",
          |  "i" : 42
          |} ]
          |""".stripMargin.trim)
  }
}

object SparkExampleBaseTest {
  case class Record(s: String, i: Int)
}

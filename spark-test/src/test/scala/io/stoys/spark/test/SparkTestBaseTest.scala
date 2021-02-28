package io.stoys.spark.test

import org.apache.spark.sql.Row

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.jdk.CollectionConverters._

class SparkTestBaseTest extends SparkTestBase {
  import SparkTestBaseTest._
  import sparkSession.implicits._

  test("file utilities") {
    assert(tmpDir.toString.contains(this.getClass.getSimpleName))

    val records = Seq(Record("foo", 42))
    val genericRecords = records.flatMap(Record.unapply)
    assert(records.size === genericRecords.size)

    writeTmpData("record.data", records)
    assert(readTmpData[Record]("record.data") === records)

    writeTmpDs("record.ds", records.toDS())
    assert(readTmpDf("record.ds").collect() === genericRecords.map(Row.fromTuple))

    val statuses = walkFileStatuses(classLevelTmpDir.toString)
    assert(statuses.keySet.filterNot(_.matches("file_utilities/record.*parquet")) === Set.empty)
    assert(statuses.size === 2)
  }

  test("writeValueAsJsonFile") {
    val records = Seq(Record("foo", 42))
    val recordsJsonPath = writeValueAsJsonFile("records.json", records)
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

object SparkTestBaseTest {
  case class Record(s: String, i: Int)
}

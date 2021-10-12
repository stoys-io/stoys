package io.stoys.spark.datasources

import io.stoys.spark.test.SparkTestBase
import io.stoys.spark.{Dfs, SToysException}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkException
import org.apache.spark.sql._

import java.nio.charset.StandardCharsets

class FilePerRowFileFormatTest extends SparkTestBase {
  import sparkSession.implicits._

  private lazy val dfs = Dfs(sparkSession)

  test("file_per_row.works") {
    val fileContents = Map(
      "foo" -> "foo",
      "dir0/dir00/bar.txt" -> "bar",
      "dir0/dir00/baz.txt" -> "baz",
      "dir0/dir01/bar.txt" -> "bar",
    )

    val textFilesPath = s"$tmpDir/file_per_row.works/text"
    val textFilesPerRow = fileContents.map(kv => TextFilePerRow(kv._1, kv._2))
    textFilesPerRow.toSeq.toDS().write.format("file_per_row").save(textFilesPath)
    assert(walkFiles(textFilesPath) === fileContents)

    val binaryFilesPath = s"$tmpDir/file_per_row.works/binary"
    val binaryFilesPerRow = fileContents.map(kv => BinaryFilePerRow(kv._1, kv._2.getBytes(StandardCharsets.UTF_8)))
    binaryFilesPerRow.toSeq.toDS().write.format("file_per_row").save(binaryFilesPath)
    assert(walkFiles(binaryFilesPath) === fileContents)
  }

  test("file_per_row.fails") {
    def write(df: DataFrame): Unit = {
      df.write.format("file_per_row").save(s"$tmpDir/file_per_row.fails")
    }

    val integerDf = Seq(("dir/file.ext", 42)).toDF("path", "content")
    interceptMessage[SToysException](write(integerDf), "Unsupported schema!")
    val incorrectNameDf = Seq(("dir/file.ext", 42)).toDF("path", "incorrectName")
    interceptMessage[SToysException](write(incorrectNameDf), "Unsupported schema!")
    val escapingDf = Seq(("../escaping_is_dangerous", "content")).toDF("path", "content")
    val escapingMessage = intercept[SparkException](write(escapingDf)).getCause.getCause.getCause.getMessage
    assert(escapingMessage.contains("has to stay in output directory"))
  }

  def walkFiles(path: String): Map[String, String] = {
    walkFileStatuses(path).map {
      case (path, status) => path -> IOUtils.toString(dfs.open(status.getPath.toString), StandardCharsets.UTF_8)
    }
  }
}

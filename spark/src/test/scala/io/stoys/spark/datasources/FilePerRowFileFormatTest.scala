package io.stoys.spark.datasources

import java.nio.charset.StandardCharsets

import io.stoys.spark.SparkTestBase
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkException
import org.apache.spark.sql._

class FilePerRowFileFormatTest extends SparkTestBase {
  import sparkSession.implicits._

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
    assert(readFiles(textFilesPath) === fileContents)

    val binaryFilesPath = s"$tmpDir/file_per_row.works/binary"
    val binaryFilesPerRow = fileContents.map(kv => BinaryFilePerRow(kv._1, kv._2.getBytes(StandardCharsets.UTF_8)))
    binaryFilesPerRow.toSeq.toDS().write.format("file_per_row").save(binaryFilesPath)
    assert(readFiles(binaryFilesPath) === fileContents)
  }

  test("file_per_row.fails") {
    def write(df: DataFrame): Unit = {
      df.write.format("file_per_row").save(s"$tmpDir/file_per_row.fails")
    }

    val integerDf = Seq(("dir/file.ext", 42)).toDF("path", "content")
    val integerMessage = intercept[SparkException](write(integerDf)).getMessage
    assert(integerMessage.contains("Unsupported schema!"))
    val incorrectNameDf = Seq(("dir/file.ext", 42)).toDF("path", "incorrectName")
    val incorrectNameMessage = intercept[SparkException](write(incorrectNameDf)).getMessage
    assert(incorrectNameMessage.contains("Unsupported schema!"))
    val escapingDf = Seq(("../escaping_is_dangerous", "content")).toDF("path", "content")
    val escapingMessage = intercept[SparkException](write(escapingDf)).getCause.getCause.getCause.getMessage
    assert(escapingMessage.contains("has to stay in output directory"))
  }

  def readFiles(path: String): Map[String, String] = {
    val fileStatusesByRelativePath = walkDfsFileStatusesByRelativePath(path)
    fileStatusesByRelativePath.mapValues(fs => IOUtils.toString(dfs.open(fs.getPath.toString), StandardCharsets.UTF_8))
  }
}

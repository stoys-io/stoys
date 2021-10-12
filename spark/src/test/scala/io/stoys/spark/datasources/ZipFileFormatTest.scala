package io.stoys.spark.datasources

import io.stoys.scala.IO
import io.stoys.spark.SToysException
import io.stoys.spark.test.SparkTestBase
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.zip.{ZipEntry, ZipFile}
import scala.jdk.CollectionConverters._

class ZipFileFormatTest extends SparkTestBase {
  import ZipFileFormat._
  import sparkSession.implicits._

  test("parseZipOptions") {
    assert(parseZipOptions(Map.empty) === ZipOptions(None, None, None))
    assert(parseZipOptions(Map("unknown_options" -> "are_ignored")) === ZipOptions(None, None, None))
    assert(parseZipOptions(Map("zip_method" -> "deflated")) === ZipOptions(Some(ZipEntry.DEFLATED), None, None))
    assert(parseZipOptions(Map("zip_method" -> "StOrEd")) === ZipOptions(Some(ZipEntry.STORED), None, None))
    assert(parseZipOptions(Map("zip_level" -> "1")) === ZipOptions(None, Some(1), None))
    assert(parseZipOptions(Map("zip_file_name" -> "foo.zip")) === ZipOptions(None, None, Some("foo.zip")))

    interceptMessage[SToysException](parseZipOptions(Map("zip_method" -> "foo")), "Unsupported")
    interceptMessage[SToysException](parseZipOptions(Map("zip_level" -> "42")), "Unsupported")
  }

  test("zip.works") {
    val fileContents = Map(
      "foo" -> "foo",
      "dir0/dir00/bar.txt" -> "bar",
      "dir0/dir00/baz.txt" -> "baz",
      "dir0/dir01/bar.txt" -> "bar",
    )

    val binaryFilesZipPath = s"$tmpDir/binary"
    val binaryFilesPerRow = fileContents.map(kv => BinaryFilePerRow(kv._1, kv._2.getBytes(StandardCharsets.UTF_8)))
    binaryFilesPerRow.toSeq.toDS().coalesce(1).write.format("zip").save(binaryFilesZipPath)
    val binaryFilesInZipFiles = walkFilesInZipFiles(binaryFilesZipPath)
    assert(binaryFilesInZipFiles.keys.map(_.endsWith(".zip")).toSeq === Seq(true))
    assert(binaryFilesInZipFiles.values.toSeq === Seq(fileContents))

    val partitionedFilesZipPath = s"$tmpDir/partitioned"
    binaryFilesPerRow.toSeq.toDS().repartition(16).write.format("zip").save(partitionedFilesZipPath)
    assert(walkFilesInZipFiles(partitionedFilesZipPath).size > 1)
  }

  test("compression") {
    val compressibleContent = "~=[,,_,,]:3" * 1024
    val ds = Seq(TextFilePerRow("kilo_cat", compressibleContent)).toDS().coalesce(1)

    val defaultOptionsZipPath = s"$tmpDir/default"
    ds.write.format("zip").save(defaultOptionsZipPath)
    val defaultFilesInZipFiles = walkFileStatuses(defaultOptionsZipPath)
    assert(defaultFilesInZipFiles.values.head.getLen < compressibleContent.length / 16)

    val storedOptionsZipPath = s"$tmpDir/stored"
    ds.write.format("zip").option("zip_method", "stored").save(storedOptionsZipPath)
    val storedFilesInZipFiles = walkFileStatuses(storedOptionsZipPath)
    assert(storedFilesInZipFiles.values.head.getLen > compressibleContent.length)
  }

  private def walkFilesInZipFiles(path: String): Map[String, Map[String, String]] = {
    val filesInZipFiles = walkFileStatuses(path).keys.map { relativePath =>
      relativePath -> IO.using(new ZipFile(s"$path/$relativePath")) { zipFile =>
        val entries = Collections.list(zipFile.entries()).asScala
        entries.map(e => e.getName -> IOUtils.toString(zipFile.getInputStream(e), StandardCharsets.UTF_8)).toMap
      }
    }
    filesInZipFiles.toMap
  }
}

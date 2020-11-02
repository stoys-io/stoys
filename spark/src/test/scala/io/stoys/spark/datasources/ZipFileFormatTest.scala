package io.stoys.spark.datasources

import java.nio.charset.StandardCharsets
import java.util.zip.{ZipEntry, ZipFile}

import io.stoys.scala.IO
import io.stoys.spark.SparkTestBase
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkException

import scala.collection.JavaConverters._

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

    val wrongMethodMessage = intercept[SparkException](parseZipOptions(Map("zip_method" -> "foo"))).getMessage
    assert(wrongMethodMessage.contains("Unsupported"))
    val wrongLevelMessage = intercept[SparkException](parseZipOptions(Map("zip_level" -> "42"))).getMessage
    assert(wrongLevelMessage.contains("Unsupported"))
  }

  test("zip.works") {
    val fileContents = Map(
      "foo" -> "foo",
      "dir0/dir00/bar.txt" -> "bar",
      "dir0/dir00/baz.txt" -> "baz",
      "dir0/dir01/bar.txt" -> "bar",
    )

    val binaryFilesZipPath = s"$tmpDir/zip.works/binary"
    val binaryFilesPerRow = fileContents.map(kv => BinaryFilePerRow(kv._1, kv._2.getBytes(StandardCharsets.UTF_8)))
    binaryFilesPerRow.toSeq.toDS().coalesce(1).write.format("zip").save(binaryFilesZipPath)
    val binaryFilesInZipFiles = readFilesInZipFiles(binaryFilesZipPath)
    assert(binaryFilesInZipFiles.keys.map(_.endsWith(".zip")).toSeq === Seq(true))
    assert(binaryFilesInZipFiles.values.toSeq === Seq(fileContents))

    val partitionedFilesZipPath = s"$tmpDir/zip.works/partitioned"
    binaryFilesPerRow.toSeq.toDS().repartition(16).write.format("zip").save(partitionedFilesZipPath)
    assert(readFilesInZipFiles(partitionedFilesZipPath).size > 1)
  }

  test("compression") {
    val compressibleContent = "~=[,,_,,]:3".repeat(1024)
    val ds = Seq(TextFilePerRow("kilo_cat", compressibleContent)).toDS().coalesce(1)

    val defaultOptionsZipPath = s"$tmpDir/compression/default"
    ds.write.format("zip").save(defaultOptionsZipPath)
    val defaultFilesInZipFiles = walkDfsFileStatusesByRelativePath(defaultOptionsZipPath)
    assert(defaultFilesInZipFiles.values.head.getLen < compressibleContent.length / 16)

    val storedOptionsZipPath = s"$tmpDir/compression/stored"
    ds.write.format("zip").option("zip_method", "stored").save(storedOptionsZipPath)
    val storedFilesInZipFiles = walkDfsFileStatusesByRelativePath(storedOptionsZipPath)
    assert(storedFilesInZipFiles.values.head.getLen > compressibleContent.length)
  }

  def readFilesInZipFiles(path: String): Map[String, Map[String, String]] = {
    val filesInZipFiles = walkDfsFileStatusesByRelativePath(path).keys.map { relativePath =>
      relativePath -> IO.using(new ZipFile(s"$path/$relativePath")) { zipFile =>
        val entries = zipFile.entries().asIterator().asScala.toSeq
        entries.map(e => e.getName -> IOUtils.toString(zipFile.getInputStream(e), StandardCharsets.UTF_8)).toMap
      }
    }
    filesInZipFiles.toMap
  }
}

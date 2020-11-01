package io.stoys.scala

import java.nio.file.{Path, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite

class TestBase extends AnyFunSuite {
  lazy val tmpDir: Path = TestBase.createNewTmpDirectoryAndCleanupOldVersions(this.getClass)
}

object TestBase {
  private val BASE_TMP_DIRECTORY = Paths.get("target", "tmp")
  private val TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")
  private val MAX_OLD_VERSIONS_TO_KEEP = 10

  private def createNewTmpDirectoryAndCleanupOldVersions(clazz: Class[_]): Path = {
    val tmpDirectoryName = s"${clazz.getSimpleName}.${TIMESTAMP_FORMATTER.format(LocalDateTime.now())}"
    val tmpDirectoryPath = BASE_TMP_DIRECTORY.resolve(tmpDirectoryName)
    tmpDirectoryPath.toFile.mkdirs()

    val oldTmpDirectoryNames = BASE_TMP_DIRECTORY.toFile.list().filter(_.startsWith(clazz.getSimpleName))
    oldTmpDirectoryNames.sorted.dropRight(MAX_OLD_VERSIONS_TO_KEEP).foreach { oldTmpDirectoryName =>
      FileUtils.deleteDirectory(BASE_TMP_DIRECTORY.resolve(oldTmpDirectoryName).toFile)
    }

    tmpDirectoryPath
  }
}

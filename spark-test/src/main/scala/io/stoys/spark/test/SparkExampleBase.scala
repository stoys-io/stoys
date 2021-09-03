package io.stoys.spark.test

import io.stoys.scala.Jackson
import org.apache.spark.sql.SparkSession

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

@Example
class SparkExampleBase extends SparkTestBase {
  private val logger = org.log4s.getLogger

  override protected def sparkSessionBuilderModifier(builder: SparkSession.Builder): Unit = {
    builder.master("local[*]")
    builder.config("spark.sql.shuffle.partitions", Runtime.getRuntime.availableProcessors())
  }

  /**
   * Write value as json string to a file.
   *
   * Note: This is not intended for fixtures but rather to save extra output from tests for debugging purposes.
   *
   * @param relativeTmpPath relative path to write to (within [[tmpDir]])
   * @param value a value to write
   * @param logFullContent should the file json content be logged as well?
   * @tparam T type of the value
   * @return [[Path]] of the file written
   */
  def writeValueAsJsonTmpFile[T](relativeTmpPath: String, value: T, logFullContent: Boolean = false): Path = {
    val path = tmpDir.resolve(relativeTmpPath)
    val valueJsonString = Jackson.json.writeValueAsString(value)
    Files.write(path, valueJsonString.getBytes(StandardCharsets.UTF_8))
    logger.info(s"File $relativeTmpPath written to:\nfile://${highlight(path.toAbsolutePath)}")
    if (logFullContent) {
      logger.info(s"File $relativeTmpPath content:\n$valueJsonString")
    }
    path
  }

  /**
   * Make the text visible in console.
   *
   * This is intended to be used to highlight important output from examples.
   *
   * @param value value to (convert to string and) highlight
   * @return highlighted color escaped string
   */
  def highlight(value: Any): String = {
    val ANSI_RESET = "\u001b[0m"
//    val ANSI_YELLOW_BOLD_BRIGHT = "\u001b[1;93m"
//    val ANSI_BLUE_BACKGROUND = "\u001b[44m"
//    s"$ANSI_YELLOW_BOLD_BRIGHT$ANSI_BLUE_BACKGROUND$value$ANSI_RESET"
//    val ANSI_BLACK = "\u001b[0;30m"
//    val ANSI_YELLOW_BACKGROUND = "\u001b[43m"
//    s"$ANSI_BLACK$ANSI_YELLOW_BACKGROUND$value$ANSI_RESET"
    val ANSI_YELLOW_BOLD_BRIGHT = "\u001b[1;93m"
    s"$ANSI_YELLOW_BOLD_BRIGHT$value$ANSI_RESET"
  }
}

package io.stoys.spark.datasources

import io.stoys.spark.test.SparkTestBase
import org.apache.spark.SparkException
import org.apache.spark.sql.types._

class FilePerRowTest extends SparkTestBase {
  import FilePerRow._

  test("findFieldIndexes") {
    val pathField = StructField("path", StringType)
    val contentField = StructField("content", StringType)
    val fieldIndexes = FieldIndexes(0, 1)

    def findIndexes(fields: StructField*): Option[FieldIndexes] = {
      findFieldIndexesOption(StructType(Seq(fields: _*)))
    }

    assert(findIndexes(pathField, contentField) === Some(fieldIndexes))
    assert(findIndexes(pathField, contentField.copy(dataType = BinaryType)) === Some(fieldIndexes))
    assert(findIndexes(pathField.copy(nullable = false), contentField.copy(nullable = false)) === Some(fieldIndexes))
    assert(findIndexes(pathField.copy(name = "PATH"), contentField.copy(name = "CoNtEnT")) === Some(fieldIndexes))
    assert(findIndexes(contentField, pathField) === Some(FieldIndexes(1, 0)))
    assert(findIndexes(pathField, contentField, StructField("extra", StringType)) === Some(fieldIndexes))

    assert(findIndexes(pathField, contentField.copy(dataType = IntegerType)) === None)
    assert(findIndexes(pathField, contentField.copy(name = "incorrect_name")) === None)
  }

  test("getCustomFilePath") {
    val basePath = "root/task_based_subdirectory_to_strip"

    assert(getCustomFilePath(basePath, "filename.ext").toString === "root/filename.ext")
    assert(getCustomFilePath(basePath, "dir/filename.ext").toString === "root/dir/filename.ext")
    assert(getCustomFilePath(basePath, "dir0/dir00/filename.ext").toString === "root/dir0/dir00/filename.ext")

    val intercepted = intercept[SparkException](getCustomFilePath(basePath, "../escaping_base_directory_is_dangerous"))
    assert(intercepted.getMessage.contains("has to stay in output directory"))
  }
}

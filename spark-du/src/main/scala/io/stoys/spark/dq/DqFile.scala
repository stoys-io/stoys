package io.stoys.spark.dq

import java.time.Instant

import io.stoys.spark.{Dfs, SToysException, SparkIO}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.{Failure, Success, Try}

private[dq] object DqFile {
  val corruptRecordField: StructField = StructField("__corrupt_record__", StringType, nullable = true)

  case class FileInput(df: DataFrame, rules: Seq[DqRule], metadata: Map[String, String])

  def openFileInputPath(sparkSession: SparkSession, inputPath: String): FileInput = {
    val dfs = Dfs(sparkSession)
    val parsedInputPath = SparkIO.parseInputPath(inputPath)
    val path = parsedInputPath.path

    if (!dfs.exists(path)) {
      throw new SToysException(s"Path '$path' does not exists.")
    }
    if (!dfs.isFile(path)) {
      throw new SToysException(s"Path '$path' is not a file.")
    }
    val status = dfs.getFileStatus(path)

    Try(createDataFrameReader(sparkSession, parsedInputPath).load(path).schema) match {
      case Failure(t) => throw new SToysException(s"Path '$path' failed to read schema (header).", t)
      case Success(schema) =>
        val df = createDataFrameReader(sparkSession, parsedInputPath)
            .schema(StructType(schema.fields :+ corruptRecordField))
            .option("columnNameOfCorruptRecord", corruptRecordField.name)
            .load(path)
        val corruptRecordRule = DqRules.namedRule("_record", "not_corrupted", s"${corruptRecordField.name} IS NULL")
        // TODO: Improve metadata for for delta, iceberg and other advanced tables?
        val metadata = Map(
          "input_path" -> inputPath,
          "path" -> path,
          "file_name" -> dfs.path(path).getName,
          "size" -> dfs.getContentSummary(path).getLength.toString,
          "modification_timestamp" -> Instant.ofEpochMilli(status.getModificationTime).toString
        )
        FileInput(df, Seq(corruptRecordRule), metadata)
    }
  }

  private def createDataFrameReader(
      sparkSession: SparkSession, parsedInputPath: SparkIO.ParsedInputPath): DataFrameReader = {
    var reader = sparkSession.read
    parsedInputPath.sosOptions.format.foreach(f => reader = reader.format(f))
    reader.options(parsedInputPath.options)
  }
}

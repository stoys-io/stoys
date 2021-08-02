package io.stoys.spark.dq

import io.stoys.spark.InputPathResolver.SosTable
import io.stoys.spark.{Dfs, InputPathResolver, SToysException, SparkIO}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant
import scala.util.{Failure, Success, Try}

private[dq] object DqFile {
  val CORRUPT_RECORD_FIELD_NAME = "__corrupt_record__"

  case class FileInput(
      df: DataFrame,
      rules: Seq[DqRule],
      metadata: Map[String, String]
  )

  def openFileInputPath(sparkSession: SparkSession, inputPath: String): FileInput = {
    val dfs = Dfs(sparkSession)
    val inputPathResolver = new InputPathResolver(dfs)

    val table = inputPathResolver.resolveInputs(inputPath) match {
      case Seq(table: SosTable) => table
      case _ => throw new SToysException(s"Input path '$inputPath' has to resolved into a single table.")
    }

    if (!dfs.exists(table.path)) {
      throw new SToysException(s"Path '${table.path}' does not exists.")
    }
    if (!dfs.getFileStatus(table.path).isFile) {
      throw new SToysException(s"Path '${table.path}' is not a file.")
    }
    val status = dfs.getFileStatus(table.path)

    val dataFrameReader = SparkIO.createDataFrameReader(sparkSession, table)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_FIELD_NAME)
    Try(dataFrameReader.load(table.path).schema) match {
      case Failure(t) => throw new SToysException(s"Failed to load '${table.path}'.", t)
      case Success(schema) =>
        val corruptRecordField = StructField(CORRUPT_RECORD_FIELD_NAME, StringType, nullable = true)
        val targetSchema = StructType(schema.fields.filter(_.name != CORRUPT_RECORD_FIELD_NAME) :+ corruptRecordField)
        val df = dataFrameReader.schema(targetSchema).load(table.path)
        val recordNotCorruptedRule = DqRules.namedRule("", "record_not_corrupted",
          s"$CORRUPT_RECORD_FIELD_NAME IS NULL")
        // TODO: Improve metadata for for delta, iceberg and other advanced tables?
        val metadata = Map(
          "input_path" -> inputPath,
          "path" -> table.path,
          "file_name" -> dfs.path(table.path).getName,
          "size" -> dfs.getContentSummary(table.path).getLength.toString,
          "modification_timestamp" -> Instant.ofEpochMilli(status.getModificationTime).toString,
          "table_name" -> table.table_name
        )
        FileInput(df, Seq(recordNotCorruptedRule), metadata)
    }
  }
}

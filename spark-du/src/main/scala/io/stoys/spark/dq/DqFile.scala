package io.stoys.spark.dq

import io.stoys.spark.InputPathResolver.SosTable
import io.stoys.spark.{Dfs, InputPathResolver, SToysException, SparkIO}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant
import scala.util.{Failure, Success, Try}

private[dq] object DqFile {
  val corruptRecordField: StructField = StructField("__corrupt_record__", StringType, nullable = true)

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

    Try(SparkIO.createDataFrameReader(sparkSession, table).load(table.path).schema) match {
      case Failure(t) => throw new SToysException(s"Path '${table.path}' failed to read schema (header).", t)
      case Success(schema) =>
        val df = SparkIO.createDataFrameReader(sparkSession, table)
            .schema(StructType(schema.fields :+ corruptRecordField))
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", corruptRecordField.name)
            .load(table.path)
        val corruptRecordRule = DqRules.namedRule("", "record_not_corrupted", s"${corruptRecordField.name} IS NULL")
        // TODO: Improve metadata for for delta, iceberg and other advanced tables?
        val metadata = Map(
          "input_path" -> inputPath,
          "path" -> table.path,
          "file_name" -> dfs.path(table.path).getName,
          "size" -> dfs.getContentSummary(table.path).getLength.toString,
          "modification_timestamp" -> Instant.ofEpochMilli(status.getModificationTime).toString
        )
        FileInput(df, Seq(corruptRecordRule), metadata)
    }
  }
}

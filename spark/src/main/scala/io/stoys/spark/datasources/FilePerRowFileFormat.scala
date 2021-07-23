package io.stoys.spark.datasources

import io.stoys.scala.IO
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType

class FilePerRowFileFormat extends FileFormat with DataSourceRegister with Serializable {
  import FilePerRow._
  import FilePerRowFileFormat._

  override def shortName(): String = "file_per_row"

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val msg = "Use 'binaryFile' file format since Spark 3.x."
    throw new UnsupportedOperationException(s"Schema inference (and file reading) is not supported! $msg")
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    // Check the schema here because supportDataType() is called per field, not per struct.
    val fieldIndexes = findFieldIndexes(dataSchema)
    new FilePerRowOutputWriterFactory(fieldIndexes, shortName())
  }
}

object FilePerRowFileFormat {
  import FilePerRow._

  private class FilePerRowOutputWriterFactory(
      fieldIndexes: FieldIndexes, fileFormatShortName: String) extends OutputWriterFactory {
    override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
      new FilePerRowOutputWriter(path, fieldIndexes, context)
    }

    override def getFileExtension(context: TaskAttemptContext): String = {
      s"${CodecConfig.from(context).getCodec.getExtension}.$fileFormatShortName"
    }
  }

  private class FilePerRowOutputWriter(
      val path: String, fieldIndexes: FieldIndexes, context: TaskAttemptContext) extends OutputWriter {
    override def write(row: InternalRow): Unit = {
      val fileDfsPath = getCustomFilePath(path, row.getString(fieldIndexes.path))
      IO.using(CodecStreams.createOutputStream(context, fileDfsPath)) { outputStream =>
        outputStream.write(row.getBinary(fieldIndexes.content))
      }
    }

    override def close(): Unit = {
    }
  }
}

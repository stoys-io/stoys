package io.stoys.spark.datasources

import java.util.Locale
import java.util.zip.{CRC32, ZipEntry, ZipOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType

import scala.util.Try

class ZipFileFormat extends FileFormat with DataSourceRegister with Serializable {
  import FilePerRow._
  import ZipFileFormat._

  override def shortName(): String = "zip"

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    throw new UnsupportedOperationException(s"Schema inference (and file reading) is not supported (yet)!")
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    // Check the schema here because supportDataType() is called per field, not per struct.
    val fieldIndexes = findFieldIndexes(dataSchema)
    new ZipOutputWriterFactory(fieldIndexes, parseZipOptions(options), shortName())
  }
}

object ZipFileFormat {
  import FilePerRow._

  private[datasources] case class ZipOptions(method: Option[Int], level: Option[Int], fileName: Option[String])

  private[datasources] def parseZipOptions(options: Map[String, String]): ZipOptions = {
    val method = options.get("zip_method").map {
      case m if m.toUpperCase(Locale.ROOT) == "STORED" => ZipEntry.STORED
      case m if m.toUpperCase(Locale.ROOT) == "DEFLATED" => ZipEntry.DEFLATED
      case m => throw new SparkException(s"Unsupported zip_method '$m'.")
    }
    val level = options.get("zip_level").map { zipLevel =>
      Try(zipLevel.toInt).toOption match {
        case Some(level) if level >= 0 && level <= 9 => level
        case _ => throw new SparkException(s"Unsupported zip_level '$zipLevel'. It has to be number 0-9.")
      }
    }
    val fileName = options.get("zip_file_name")
    ZipOptions(method, level, fileName)
  }

  private class ZipOutputWriterFactory(
      fieldIndexes: FieldIndexes, zipOptions: ZipOptions, fileFormatShortName: String) extends OutputWriterFactory {
    override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
      new ZipOutputWriter(fieldIndexes, zipOptions, path, context.getConfiguration)
    }

    override def getFileExtension(context: TaskAttemptContext): String = {
      s".$fileFormatShortName"
    }
  }

  private class ZipOutputWriter(fieldIndexes: FieldIndexes, zipOptions: ZipOptions, path: String,
      configuration: Configuration) extends OutputWriter {
    private val dfsPath = zipOptions.fileName.map(fn => getCustomFilePath(path, fn)).getOrElse(new Path(path))
    private val outputStream = dfsPath.getFileSystem(configuration).create(dfsPath, false)
    private val zipOutputStream = new ZipOutputStream(outputStream)

    zipOptions.method.foreach(zipOutputStream.setMethod)
    zipOptions.level.foreach(zipOutputStream.setLevel)

    private def setupStoredZipEntry(zipEntry: ZipEntry, content: Array[Byte]): Unit = {
      zipEntry.setSize(content.length)
      zipEntry.setCompressedSize(content.length)
      val crc32 = new CRC32()
      crc32.update(content)
      zipEntry.setCrc(crc32.getValue)
    }

    override def write(row: InternalRow): Unit = {
      val path = row.getString(fieldIndexes.path)
      val content = row.getBinary(fieldIndexes.content)
      val zipEntry = new ZipEntry(path)
      if (zipOptions.method.contains(ZipEntry.STORED)) {
        setupStoredZipEntry(zipEntry, content)
      }
      zipOutputStream.putNextEntry(zipEntry)
      zipOutputStream.write(content)
      zipOutputStream.closeEntry()
    }

    override def close(): Unit = {
      zipOutputStream.close()
      outputStream.close()
    }
  }
}

package io.stoys.spark.test

import java.nio.file.{Path, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.reflect.runtime.universe._

abstract class SparkTestBase extends AnyFunSuite {
  /**
   * Local temporary directory. [[SparkTestBase.MAX_OLD_VERSIONS_TO_KEEP]] version will be and kept accessible
   * in [[SparkTestBase.BASE_TMP_DIRECTORY]] after test execution is finished. It is intended to help with debugging.
   */
  protected lazy val tmpDir: Path = SparkTestBase.createTemporaryDirectoryAndCleanupOldVersions(this.getClass)

  /**
   * [[sparkSessionBuilderModifier]] can be used to modify SparkSession after default config is applied.
   *
   * BEWARE: Spark currently cache [[SparkSession]] and once it is create it stays the same within the jvm run.
   * Any modifications in second unit test (after first one is run) will not have nay effect!
   *
   * @param builder the [[SparkSession.Builder]] to modify
   */
  protected def sparkSessionBuilderModifier(builder: SparkSession.Builder): Unit = {
  }

  protected lazy val sparkSession: SparkSession = {
    val builder = SparkSession.builder()
    builder.master("local[1]")
    builder.appName(this.getClass.getName.stripSuffix("$"))
    builder.config("spark.ui.enabled", "false")
    builder.config("spark.master.rest.enabled", "false")
    builder.config("spark.sql.shuffle.partitions", "1")
    builder.config("spark.sql.warehouse.dir", "target/spark-warehouse")
//    builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSessionBuilderModifier(builder)
    builder.getOrCreate()
  }

  def readDataFrame(path: String): DataFrame = {
    sparkSession.read.format("parquet").load(path)
  }

  def readDataset[T <: Product : TypeTag](path: String): Dataset[T] = {
    import sparkSession.implicits._
    readDataFrame(path).as[T]
  }

  def readData[T <: Product : TypeTag](path: String): Seq[T] = {
    readDataset[T](path).collect()
  }

  def writeDataFrame(path: String, df: DataFrame): Unit = {
    df.write.format("parquet").mode(SaveMode.Overwrite).save(path)
  }

  def writeDataset[T](path: String, ds: Dataset[T]): Unit = {
    writeDataFrame(path, ds.toDF())
  }

  def writeData[T <: Product : TypeTag](path: String, data: Seq[T]): Unit = {
    import sparkSession.implicits._
    writeDataset[T](path, sparkSession.createDataset(data))
  }

  /**
   * Recursively find all files within given path. It will omit spark/hadoop _SUCCESS and *.crc files.
   *
   * @param path file name or bse directory to visit
   * @return Map relative paths as keys and [[FileStatus]] as values
   */
  def walkDfsFileStatusesByRelativePath(path: String): Map[String, FileStatus] = {
    val dfsPath = new org.apache.hadoop.fs.Path(path)
    val fs = dfsPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val qualifiedPath = dfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val fileStatuses = mutable.Buffer.empty[FileStatus]
    var stack = List(qualifiedPath)
    while (stack.nonEmpty) {
      val (directories, files) = fs.listStatus(stack.head).partition(_.isDirectory)
      stack = directories.map(_.getPath).toList ++ stack.tail
      fileStatuses ++= files
    }
    val relativePathsToFileStatuses = fileStatuses.flatMap {
      case fs if fs.getPath.getName == "_SUCCESS" || fs.getPath.getName.endsWith(".crc") => None
      case fs => Some(fs.getPath.toString.stripPrefix(s"$qualifiedPath/") -> fs)
    }
    relativePathsToFileStatuses.toMap
  }
}

object SparkTestBase {
  val BASE_TMP_DIRECTORY: Path = Paths.get("target", "tmp")
  val TIMESTAMP_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")
  val MAX_OLD_VERSIONS_TO_KEEP: Int = 10

  def createTemporaryDirectoryAndCleanupOldVersions(clazz: Class[_]): Path = {
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

package io.stoys.spark.test

import io.stoys.scala.Strings
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterEachTestData, TestData}

import java.nio.file.{Path, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.reflect.runtime.universe._

abstract class SparkTestBase extends AnyFunSuite with BeforeAndAfterEachTestData {
  import SparkTestBase._

  protected val defaultDataSourceFormat: String = "parquet"
  protected val defaultDataSourceOptions: Map[String, String] = Map.empty

  /**
   * Local temporary directory (per test class). [[MAX_OLD_TMP_VERSIONS_TO_KEEP]] version will be and kept accessible
   * in [[BASE_TMP_DIRECTORY]] after test execution is finished. It is intended to help with debugging.
   *
   * Note: Prefer using tmpDir function - it will return fresh subdirectory per test function.
   */
  protected lazy val classLevelTmpDir: Path = createTemporaryDirectoryAndCleanupOldVersions(this.getClass)

  /**
   * ScalaTest's [[TestData]] update before run of each test function.
   */
  protected var testData: TestData = _

  override protected def beforeEach(testData: TestData): Unit = {
    this.testData = testData
  }

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

  /**
   * [[SparkSession]] to use in the test.
   *
   * One can override [[sparkSessionBuilderModifier]] inside a test class for customization.
   */
  protected lazy val sparkSession: SparkSession = {
    val builder = SparkSession.builder()
    builder.master("local[1]")
    builder.appName(this.getClass.getName.stripSuffix("$"))
    builder.config("spark.master.rest.enabled", "false")
//    builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    builder.config("spark.sql.codegen.wholeStage", "false")
    builder.config("spark.sql.shuffle.partitions", "1")
    builder.config("spark.sql.warehouse.dir", "target/spark-warehouse")
    builder.config("spark.ui.enabled", "false")
    sparkSessionBuilderModifier(builder)
    builder.getOrCreate()
  }

  /**
   * Temporary directory path. This path is unique not only per test class but even per test function.
   */
  def tmpDir: Path = {
    val testName = Option(testData).map(_.name).getOrElse("_")
    val tmpDir = classLevelTmpDir.resolve(Strings.toWordCharactersCollapsing(testName))
    tmpDir.toFile.mkdirs()
    tmpDir
  }

  def readTmpDf(relativeTmpPath: String,
      format: String = defaultDataSourceFormat, options: Map[String, String] = defaultDataSourceOptions): DataFrame = {
    val path = tmpDir.resolve(relativeTmpPath)
    sparkSession.read.format(format).options(options).load(path.toString)
  }

  def readTmpData[T <: Product : TypeTag](relativeTmpPath: String,
      format: String = defaultDataSourceFormat, options: Map[String, String] = defaultDataSourceOptions): Seq[T] = {
    import sparkSession.implicits._
    readTmpDf(relativeTmpPath, format, options).as[T].collect()
  }

  def writeTmpDs(relativeTmpPath: String, ds: Dataset[_],
      format: String = defaultDataSourceFormat, options: Map[String, String] = defaultDataSourceOptions): Path = {
    val path = tmpDir.resolve(relativeTmpPath)
    ds.write.format(format).options(options).save(path.toString)
    path
  }

  def writeTmpData[T <: Product : TypeTag](relativeTmpPath: String, data: Seq[T],
      format: String = defaultDataSourceFormat, options: Map[String, String] = defaultDataSourceOptions): Path = {
    import sparkSession.implicits._
    val df = sparkSession.createDataset(data)
    writeTmpDs(relativeTmpPath, df, format, options)
  }

  /**
   * Recursively find all files within given path.
   *
   * @param path file name or bse directory to visit
   * @param omitMetadataFiles omit spark/hadoop _SUCCESS and *.crc files (default: true)
   * @return Map relative paths as keys and [[FileStatus]] as values
   */
  def walkFileStatuses(path: String, omitMetadataFiles: Boolean = true): Map[String, FileStatus] = {
    SparkTestBase.walkFileStatuses(sparkSession.sparkContext.hadoopConfiguration, path, omitMetadataFiles)
  }

  /**
   * Quote identifier if needed.
   *
   * Note: Spark was always quoting column names in [[org.apache.spark.sql.catalyst.expressions.Expression.sql]]
   * before Spark 3.2. Since then identifiers are quoted only when needed.
   *
   * @param part identifier part to quote
   * @return raw or quoted identifier
   */
  protected def quoteIfNeeded(part: String): String = {
    if (UNQUOTE_SAFE_IDENTIFIER_PATTERN.findFirstMatchIn(part).isEmpty || sparkSession.version < "3.2") {
      quoteIdentifier(part)
    } else {
      part
    }
  }
}

object SparkTestBase {
  val TIMESTAMP_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")
  val BASE_TMP_DIRECTORY: Path = Paths.get("target", "tmp")
  val MAX_OLD_TMP_VERSIONS_TO_KEEP: Int = 10
  private val UNQUOTE_SAFE_IDENTIFIER_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_]*$".r

  def createTemporaryDirectoryAndCleanupOldVersions(clazz: Class[_]): Path = {
    val tmpDirName = s"${clazz.getSimpleName}.${TIMESTAMP_FORMATTER.format(LocalDateTime.now())}"
    val tmpDirPath = BASE_TMP_DIRECTORY.resolve(tmpDirName).toAbsolutePath
    tmpDirPath.toFile.mkdirs()

    val currentTmpDirNames = BASE_TMP_DIRECTORY.toFile.list().filter(_.startsWith(clazz.getSimpleName))
    val deprecatedTmpDirNames = currentTmpDirNames.sorted.dropRight(MAX_OLD_TMP_VERSIONS_TO_KEEP)
    deprecatedTmpDirNames.foreach(tdn => FileUtils.deleteDirectory(BASE_TMP_DIRECTORY.resolve(tdn).toFile))

    tmpDirPath
  }

  /**
   * Recursively find all files within given path.
   *
   * @param hadoopConfiguration hadoop configuration [[HadoopConfiguration]]
   * @param path file name or bse directory to visit
   * @param skipMetadataFiles skip spark/hadoop _SUCCESS and *.crc files
   * @return Map relative paths as keys and [[FileStatus]] as values
   */
  def walkFileStatuses(hadoopConfiguration: HadoopConfiguration,
      path: String, skipMetadataFiles: Boolean): Map[String, FileStatus] = {
    val dfsPath = new org.apache.hadoop.fs.Path(path)
    val fs = dfsPath.getFileSystem(hadoopConfiguration)
    val qualifiedPath = dfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val fileStatuses = mutable.Buffer.empty[FileStatus]
    var stack = List(qualifiedPath)
    while (stack.nonEmpty) {
      val (directories, files) = fs.listStatus(stack.head).partition(_.isDirectory)
      stack = directories.map(_.getPath).toList ++ stack.tail
      fileStatuses ++= files
    }
    val relativePathsToFileStatuses = fileStatuses.flatMap {
      case fs if fs.getPath.getName == "_SUCCESS" && skipMetadataFiles => None
      case fs if fs.getPath.getName.endsWith(".crc") && skipMetadataFiles => None
      case fs => Some(fs.getPath.toString.stripPrefix(s"$qualifiedPath/") -> fs)
    }
    relativePathsToFileStatuses.toMap
  }
}

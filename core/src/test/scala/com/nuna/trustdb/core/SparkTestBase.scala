package com.nuna.trustdb.core

import java.nio.file.{Files, Path, Paths}
import java.sql.Date
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.nuna.trustdb.core.spark.Dfs
import org.apache.spark.sql._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class SparkTestBase extends AnyFunSuite with BeforeAndAfterAll {
  lazy val sparkSession = SparkSession.builder()
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.master.rest.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .appName(this.getClass.getName.stripSuffix("$"))
      .getOrCreate()

  lazy val dfs = new Dfs(sparkSession)

  private[SparkTestBase] val localTempDirectories = mutable.Buffer.empty[Path]

  override def afterAll() {
//    localTempDirectories.foreach(_.toFile.delete())
    super.afterAll()
  }

  def assertDatasetEquality[T](actual: Dataset[T], expected: Dataset[T], ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false, ignoreOrdering: Boolean = true): Unit = {
    SparkTestBase.comparer.assertSmallDatasetEquality(actual, expected, ignoreNullable = ignoreNullable,
      ignoreColumnNames = ignoreColumnNames, orderedComparison = !ignoreOrdering)
  }

  def assertDataFrameEquality(actual: DataFrame, expected: DataFrame, ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false, ignoreOrdering: Boolean = true, precision: Double = 0.0): Unit = {
    if (precision == 0.0) {
      SparkTestBase.comparer.assertSmallDataFrameEquality(actual, expected, ignoreNullable = ignoreNullable,
        ignoreColumnNames = ignoreColumnNames, orderedComparison = !ignoreOrdering)
    } else {
      SparkTestBase.comparer.assertApproximateDataFrameEquality(actual, expected, precision = precision,
        ignoreNullable = ignoreNullable, ignoreColumnNames = ignoreColumnNames, orderedComparison = !ignoreOrdering)
    }
  }

  def createLocalTempDirectory(): Path = {
    val tmpDirectoryPath = Paths.get("target", "tmp")
    tmpDirectoryPath.toFile.mkdirs()
    val prefix = s"${this.getClass.getSimpleName}.${SparkTestBase.TIMESTAMP_FORMATTER.format(LocalDateTime.now())}."
    val directory = Files.createTempDirectory(tmpDirectoryPath, prefix)
      localTempDirectories += directory
    directory
  }

  def readDataFrame(path: String): DataFrame = {
    sparkSession.read.format("parquet").load(path)
  }

  def readDataset[T: Encoder](path: String): Dataset[T] = {
    readDataFrame(path).as[T]
  }

  def readData[T: Encoder](path: String): Seq[T] = {
    readDataset[T](path).collect()
  }

  def writeDataFrame(path: String, df: DataFrame): Unit = {
    df.write.format("parquet").mode(SaveMode.Overwrite).save(path)
  }

  def writeDataset[T](path: String, ds: Dataset[T]): Unit = {
    writeDataFrame(path, ds.toDF())
  }

  def writeData[T: Encoder](path: String, data: Seq[T]): Unit = {
    writeDataset[T](path, sparkSession.createDataset(data))
  }
}

object SparkTestBase {
  private[SparkTestBase] val TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")
  private[SparkTestBase] val comparer = new DataFrameComparer {}

  // BEWARE: Dangerous and powerful implicits lives here! Be careful what we add here.
  // Do NOT copy this to src/main! It does not belong to production code. It is for tests only!
  object implicits {
    import scala.language.implicitConversions

    implicit def toOption[T](value: T): Option[T] = Some(value)

    implicit def toDate(date: String): Date = Date.valueOf(date)
  }
}

package com.nuna.trustdb.core

import java.sql.Date

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.nuna.trustdb.core.spark.Dfs
import com.nuna.trustdb.core.util.TestBase
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql._

import scala.collection.mutable

class SparkTestBase extends TestBase {
  lazy val sparkSession: SparkSession = SparkSession.builder()
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.master.rest.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .appName(this.getClass.getName.stripSuffix("$"))
      .getOrCreate()

  lazy val dfs: Dfs = Dfs(sparkSession)

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

  def walkDfsFileStatusesByRelativePath(path: String): Map[String, FileStatus] = {
    val (fs, basePath) = dfs.asQualifiedPath(path)
    val fileStatuses = mutable.Buffer.empty[FileStatus]
    var stack = List(basePath)
    while (stack.nonEmpty) {
      val (directories, files) = fs.listStatus(stack.head).partition(_.isDirectory)
      stack = directories.map(_.getPath).toList ++ stack.tail
      fileStatuses ++= files
    }
    val relativePathsToFileStatuses = fileStatuses.flatMap {
      case fs if fs.getPath.getName == "_SUCCESS" || fs.getPath.getName.endsWith(".crc") => None
      case fs => Some(fs.getPath.toString.stripPrefix(s"$basePath/") -> fs)
    }
    relativePathsToFileStatuses.toMap
  }
}

object SparkTestBase {
  private[SparkTestBase] val comparer = new DataFrameComparer {}

  // BEWARE: Dangerous and powerful implicits lives here! Be careful what we add here.
  // Do NOT copy this to src/main! It does not belong to production code. It is for tests only!
  object implicits {
    import scala.language.implicitConversions

    implicit def toOption[T](value: T): Option[T] = Some(value)

    implicit def toDate(date: String): Date = Date.valueOf(date)
  }
}

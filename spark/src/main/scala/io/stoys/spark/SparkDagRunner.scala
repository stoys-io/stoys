package io.stoys.spark

import io.stoys.scala.{Configuration, IO}
import io.stoys.utils.Spark
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Timestamp
import scala.collection.mutable
import scala.reflect.runtime.universe._

class SparkDagRunner(sparkSession: SparkSession, sparkIO: SparkIO, config: SparkDagRunnerConfig) {
  private val logger = org.log4s.getLogger

  private val metricDatasets = mutable.Buffer.empty[Dataset[Metric]]

  def read[T <: Product : TypeTag]: Dataset[T] = {
    read[T](TableName[T])
  }

  def read[T <: Product](tableName: TableName[T]): Dataset[T] = {
    sparkIO.ds[T](tableName)
  }

  def wrap[T <: Product : TypeTag](ds: Dataset[T], logicalName: String = null): Dataset[T] = {
    wrap(ds, TableName[T](logicalName))
  }

  def wrap[T <: Product : TypeTag](ds: Dataset[T], tableName: TableName[T]): Dataset[T] = {
    val fullTableName = tableName.fullTableName()
    val result = sparkIO.getInputTable(fullTableName) match {
      case Some(sosTable) =>
        logger.warn(s"Overriding $tableName from ${sosTable.path}!")
        sparkIO.ds[T](tableName)
      case None =>
        if (config.debug || config.compute_collections.contains(fullTableName)) {
          logger.info(s"Computing and writing $fullTableName")
          sparkIO.write(ds, tableName)
        }
        ds
    }
    cached(result)
  }

  def wrapMetrics(metrics: Dataset[Metric]): Unit = {
    metricDatasets.append(metrics)
  }

  def run(mainDag: Runnable): Unit = {
    val mainDagRunTry = scala.util.Try(mainDag.run())

    val mergedMetrics = metricDatasets.reduceOption((m1, m2) => m1.union(m2)).map(cached)
    mergedMetrics.foreach(mm => sparkIO.write(mm, TableName[Metric]))

    mainDagRunTry match {
      case scala.util.Success(_) =>
        writeSharedOutputPath(mergedMetrics)
      case scala.util.Failure(t) =>
        logger.error(t)(s"Running main dag ${mainDag.getClass} has failed!")
        throw t
    }
  }

  private def cached[T](ds: Dataset[T]): Dataset[T] = {
    if (config.disable_caching) {
      ds
    } else {
      ds.cache()
    }
  }

  private def writeSharedOutputPath(mergedMetrics: Option[Dataset[Metric]]): Unit = {
    config.shared_output_path.foreach { sharedOutputPath =>
      if (Spark.isDeltaSupported) {
        mergedMetrics.foreach { mm =>
          mm.withColumn("run_timestamp", lit(Timestamp.valueOf(config.run_timestamp)))
              .write.format("delta").mode("append").save(s"$sharedOutputPath/metric")
        }
      } else {
        logger.error(s"Delta file format is not supported. Shared metrics are not written!")
      }
      sparkIO.writeSymLink(s"$sharedOutputPath/latest.list")
    }
  }
}

object SparkDagRunner {
  def main(args: Array[String]): Unit = {
    val configuration = Configuration(args)
    val sparkConfig = configuration.readConfig[SparkConfig]
    val sparkDagRunnerConfig = configuration.readConfig[SparkDagRunnerConfig]
    val sparkSession = SparkUtils.createSparkSession(sparkConfig)
    val sparkIOConfig = configuration.readConfig[SparkIOConfig]

    IO.using(new SparkIO(sparkSession, sparkIOConfig)) { sparkIO =>
      try {
        val sparkDagRunner = new SparkDagRunner(sparkSession, sparkIO, sparkDagRunnerConfig)
        // Main dag has to take the following arguments in constructor (exact types and order) and implement Runnable.
        val mainDag = Class.forName(sparkDagRunnerConfig.main_dag_class)
            .getDeclaredConstructor(classOf[Configuration], classOf[SparkSession], classOf[SparkDagRunner])
            .newInstance(configuration, sparkSession, sparkDagRunner)
            .asInstanceOf[Runnable]
        sparkDagRunner.run(mainDag)
      } finally {
        sparkSession.sqlContext.clearCache()
      }
    }
  }
}

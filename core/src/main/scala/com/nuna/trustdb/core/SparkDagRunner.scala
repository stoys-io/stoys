package com.nuna.trustdb.core

import java.sql.Timestamp

import com.nuna.trustdb.core.spark.{SparkIO, SparkIOConfig, SparkUtils, TableName}
import com.nuna.trustdb.core.util.{Configuration, IO}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.collection.mutable
import scala.reflect.runtime.universe._

class SparkDagRunner(sparkSession: SparkSession, sparkIO: SparkIO, config: SparkDagRunnerConfig) {
  import sparkSession.implicits._

  private val logger = org.log4s.getLogger

  private val metricDatasets = mutable.Buffer.empty[Dataset[Metric]]

  def read[T <: Product : TypeTag]: Dataset[T] = {
    read[T](TableName[T])
  }

  def read[T <: Product](tableName: TableName[T]): Dataset[T] = {
    implicit val typeTagT = tableName.typeTag
    sparkIO.ds[T](tableName)
  }

  def wrap[T <: Product : TypeTag](ds: Dataset[T], logicalName: String = null): Dataset[T] = {
    wrap(ds, TableName[T](logicalName))
  }

  def wrap[T <: Product : Encoder](ds: Dataset[T], tableName: TableName[T]): Dataset[T] = {
    val fullTableName = tableName.fullTableName()
    sparkIO.getInputTable(fullTableName) match {
      case Some(sosTable) =>
        logger.warn(s"Overriding $tableName from ${sosTable.path}!")
        sparkIO.ds[T](tableName)
      case None =>
        if (config.debug || config.computeCollections.contains(fullTableName)) {
          logger.info(s"Computing and writing $fullTableName")
          sparkIO.write(ds, tableName)
        }
        ds
    }
  }

  def wrapMetrics(metrics: Dataset[Metric]): Unit = {
    metricDatasets.append(metrics)
  }

  def finish(): Unit = {
    val mergedMetrics = metricDatasets.reduceOption((m1, m2) => m1.union(m2))

    mergedMetrics.foreach(mm => sparkIO.write(mm, TableName[Metric]))

    config.sharedOutputPath.foreach { sharedOutputPath =>
      mergedMetrics.foreach { mm =>
        mm.withColumn("run_timestamp", lit(Timestamp.valueOf(config.runTimestamp)))
            .write.format("delta").mode("append").save(s"$sharedOutputPath/metric")
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
      sparkIO.init()
      val sparkDagRunner = new SparkDagRunner(sparkSession, sparkIO, sparkDagRunnerConfig)
      // Main dag has to take the following arguments in constructor (exact types and order) and implement Runnable.
      val mainDag = Class.forName(sparkDagRunnerConfig.mainDagClass)
          .getDeclaredConstructor(classOf[Configuration], classOf[SparkSession], classOf[SparkDagRunner])
          .newInstance(configuration, sparkSession, sparkDagRunner)
          .asInstanceOf[Runnable]
      mainDag.run()
      sparkDagRunner.finish()
    }
  }
}

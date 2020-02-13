package com.nuna.trustdb.core

import com.nuna.trustdb.core.spark.{SparkIO, SparkIOConfig, SparkUtils, TableName}
import com.nuna.trustdb.core.util.Configuration
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.collection.mutable
import scala.reflect.runtime.universe._

class SparkDagRunner(sparkSession: SparkSession, sparkIO: SparkIO, sparkDagRunnerConfig: SparkDagRunnerConfig) {
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
    sparkIO.inputTables.find(_.fullTableName == fullTableName) match {
      case Some(dsFromInputTables) =>
        logger.warn(s"Overriding $tableName from ${dsFromInputTables.path}!")
        //NOTE: ignore computing metrics for overriding collections
        sparkIO.ds[T](tableName)
      case None =>
        if (sparkDagRunnerConfig.debug || sparkDagRunnerConfig.computeCollections.contains(fullTableName)) {
          logger.info(s"Computing and writing $fullTableName")
          sparkIO.write(ds, tableName)
        }
        ds
    }
  }

  def wrapMetrics(metrics: Dataset[Metric]): Unit = {
    metricDatasets.append(metrics)
  }

  private def finish(): Unit = {
    sparkIO.writeMetrics(metricDatasets, sparkDagRunnerConfig.runTimestamp)
    sparkIO.writeInputOutputTableList()
  }
}

object SparkDagRunner {
  def main(args: Array[String]): Unit = {
    val configuration = new Configuration(args)
    val sparkConfig = configuration.readConfig[SparkConfig]
    val sparkDagRunnerConfig = configuration.readConfig[SparkDagRunnerConfig]
    val sparkSession = SparkUtils.createSparkSession(sparkConfig)
    val sparkIOConfig = configuration.readConfig[SparkIOConfig]
    val sparkIO = new SparkIO(sparkSession, sparkIOConfig)
    val sparkDagRunner = new SparkDagRunner(sparkSession, sparkIO, sparkDagRunnerConfig)

    // Note: Main dag has to take the following arguments in constructor (exact types and order) and implement Runnable.
    val mainDag = Class.forName(sparkDagRunnerConfig.mainDagClass)
        .getDeclaredConstructor(classOf[Configuration], classOf[SparkSession], classOf[SparkDagRunner])
        .newInstance(configuration, sparkSession, sparkDagRunner)
        .asInstanceOf[Runnable]

    mainDag.run()
    sparkDagRunner.finish()
  }
}

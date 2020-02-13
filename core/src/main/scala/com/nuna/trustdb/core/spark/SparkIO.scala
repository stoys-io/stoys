package com.nuna.trustdb.core.spark

import java.net.URI
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.nuna.trustdb.core.Metric
import com.nuna.trustdb.core.util.IO
import org.apache.commons.io.IOUtils
import org.apache.http.client.utils.{URIBuilder, URLEncodedUtils}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

class SparkIO(sparkSession: SparkSession, config: SparkIOConfig) {
  import SparkIO._

  private val logger = org.log4s.getLogger

  private val dfs = new Dfs(sparkSession)

  val inputTables = config.inputPaths.flatMap(resolveInputTables)
  var outputTables = mutable.Buffer.empty[String]

  inputTables.foreach(registerInputTable)
  Option(config.outputPath).map(outPath => IO.using(dfs.create(s"$outPath/input.list")) { dos =>
    IOUtils.write(inputTables.map(_.toURLString()).mkString("\n"), dos)
  })

  def df[T <: Product](tableName: TableName[T]): DataFrame = {
    sparkSession.table(tableName.fullTableName())
  }

  def ds[T <: Product : Encoder](tableName: TableName[T]): Dataset[T] = {
    df(tableName).as[T]
  }

  private def writeDF(df: DataFrame, fullTableName: String): String = {
    val path = s"${config.outputPath}/$fullTableName"
    logger.info(s"Writing $fullTableName to $path.")
    df.write.format(config.writeFormat).mode(config.writeMode).options(config.writeOptions).save(path)
    val readConfig = ReaderConfig(fullTableName, path, config.writeFormat, config.writeOptions).toURLString()
    outputTables.append(readConfig)
    readConfig
  }

  def write[T <: Product](ds: Dataset[T], tableName: TableName[T]): String = {
    writeDF(ds.toDF(), tableName.fullTableName())
  }

  def writeMetrics(metrics: Seq[Dataset[Metric]], run_timestamp: LocalDateTime): Unit = {
    val mergedMetrics = metrics.reduceOption((m1, m2) => m1.union(m2))
    mergedMetrics.foreach(mm => write(mm, TableName[Metric]))

    mergedMetrics.map(mm =>
      config.sharedOutputPath.map(publishPath =>
        mm.withColumn("run_timestamp", lit(Timestamp.valueOf(run_timestamp)))
            .write.format("delta").mode("append").save(s"$publishPath/metric")
      ))
  }

  def writeInputOutputTableList(): Unit = {
    writeInputOutputTableList(s"${config.outputPath}/io.list")
    config.sharedOutputPath.map(sharedPath => writeInputOutputTableList(s"$sharedPath/latest.io.list"))
  }

  def writeInputOutputTableList(filePath: String): Unit = {
    IO.using(dfs.create(filePath)) { dos =>
      IOUtils.write(outputTables.mkString("\n"), dos)
      IOUtils.write("\n#associated input tables\n", dos)
      IOUtils.write(inputTables.map(_.toURLString()).mkString("\n"), dos)
    }
  }

  private[spark] def resolveInputTables(inputPath: String): Seq[ReaderConfig] = {
    val (path, params) = parsePathParams(inputPath)
    val (rawSosParams, sparkParams) = params.toMap.partition(_._1.toLowerCase(Locale.ROOT).startsWith(SOS_PREFIX))
    val sosParams = toSosParams(rawSosParams)

    val lastPathSegment = dfs.path(path).getName
    val nameAndListingMsg = s"${SOS_PREFIX}table_name and ${SOS_PREFIX}listing_strategy"
    val inputTables = (lastPathSegment, sosParams.tableName, sosParams.listingStrategy) match {
      case (_, Some(_), Some(_)) =>
        throw new IllegalArgumentException(s"Having both $nameAndListingMsg at the same time are not supported ($path)")
      case (SOS_LIST_PATTERN(_), Some(_), _) | (SOS_LIST_PATTERN(_), _, Some(_)) =>
        throw new IllegalArgumentException(s"Parameters $nameAndListingMsg are not supported *.list files ($path).")
      case (SOS_LIST_PATTERN(_), None, None) =>
        val content = IO.using(dfs.open(path)) { is => IOUtils.toString(is, StandardCharsets.UTF_8) }
        // TODO: async, cycles, max depth
        content.split('\n').filterNot(_.startsWith("#")).flatMap(resolveInputTables).toSeq
      case (_, None, Some(strategy)) => listTables(strategy, path, sosParams.copy(listingStrategy = None), sparkParams)
      case (lastPathSegment, None, None) => Seq(ReaderConfig(lastPathSegment, path, sosParams.format, sparkParams))
      case (_, Some(tableName), None) => Seq(ReaderConfig(tableName, path, sosParams.format, sparkParams))
    }
    inputTables.sortBy(_.fullTableName)
  }

  private[spark] def listTables(strategy: String, path: String, sosParams: SosParams,
      sparkParams: Map[String, String]): Seq[ReaderConfig] = {
    val statuses = dfs.listStatus(path)
    strategy match {
      case "@" => statuses.map(s => ReaderConfig(s.getPath.getName, s.getPath.toString, sosParams.format, sparkParams))
      // TODO: add timestamp based strategies
    }
  }

  private[spark] def registerInputTable(inputTable: ReaderConfig): DataFrame = {
    logger.info(s"Registering input table $inputTable")
    val reader = sparkSession.read.format(inputTable.format).options(inputTable.options)
    val table = reader.load(inputTable.path)
    table.createOrReplaceTempView(inputTable.fullTableName)
    table
  }
}

object SparkIO {
  val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  val SOS_PREFIX = "sos-"
  val SOS_LIST_PATTERN = "(?i)(.*)\\.list".r
  val DEFAULT_FORMAT = "parquet"

  case class SosParams(format: String, tableName: Option[String], listingStrategy: Option[String])

  case class ReaderConfig(fullTableName: String, path: String, format: String, options: Map[String, String]) {
    def toURLString(): String = {
      val uriBuilder = new URIBuilder(path)
          .addParameter(s"${SOS_PREFIX}format", format)
          .addParameter(s"${SOS_PREFIX}table_name", fullTableName)
      options.foreach(kv => uriBuilder.addParameter(kv._1, kv._2))
      uriBuilder.build().toString
    }
  }

  private[spark] def parsePathParams(pathParamsString: String): (String, Seq[(String, String)]) = {
    val pathParamsUri = new URI(pathParamsString)
    val path = new URIBuilder(pathParamsUri).removeQuery().build().toString
    val nameValuePairs = URLEncodedUtils.parse(pathParamsUri, StandardCharsets.UTF_8.name())
    val params = nameValuePairs.asScala.map(kv => kv.getName -> kv.getValue)
    (path, params)
  }

  private def toSosParams(params: Map[String, String]): SosParams = {
    val normalizedSosParams = params.map(kv => (kv._1.toLowerCase(Locale.ROOT).stripPrefix(SOS_PREFIX), kv._2))
    SosParams(
      format = normalizedSosParams.getOrElse("format", DEFAULT_FORMAT),
      tableName = normalizedSosParams.get("table_name"),
      listingStrategy = normalizedSosParams.get("listing_strategy").map(_.toLowerCase(Locale.ROOT)))
  }
}

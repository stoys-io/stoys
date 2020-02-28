package com.nuna.trustdb.core.spark

import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.nuna.trustdb.core.util.IO
import org.apache.commons.io.IOUtils
import org.apache.http.client.utils.{URIBuilder, URLEncodedUtils}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

class SparkIO(sparkSession: SparkSession, val config: SparkIOConfig) extends AutoCloseable {
  import SparkIO._

  private val logger = org.log4s.getLogger

  val dfs = new Dfs(sparkSession)

  val inputPaths = mutable.Buffer.empty[String]
  val resolvedInputTables = mutable.Buffer.empty[SosTable]
  val outputTables = mutable.Buffer.empty[SosTable]

  def init(): Unit = {
    addInputPaths(config.inputPaths: _*)
  }

  def addInputPaths(paths: String*): Unit = {
    paths.foreach { path =>
      inputPaths.append(path)
      val tables = resolveInputTables(path)
      resolvedInputTables.appendAll(tables)
      tables.foreach(registerInputTable)
    }
  }

  def df[T <: Product](tableName: TableName[T]): DataFrame = {
    sparkSession.table(tableName.fullTableName())
  }

  def ds[T <: Product : Encoder](tableName: TableName[T]): Dataset[T] = {
    df(tableName).as[T]
  }

  private def writeDF(df: DataFrame, fullTableName: String): Unit = {
    val path = s"${config.outputPath.get}/$fullTableName"
    logger.info(s"Writing $fullTableName to $path.")
    var writer = df.write
    config.writeFormat.foreach(f => writer = writer.format(f))
    config.writeMode.foreach(m => writer = writer.mode(m))
    writer = writer.options(config.writeOptions)
    writer.save(path)
    val table = SosTable(fullTableName, path, config.writeFormat, config.writeOptions)
    outputTables.append(table)
  }

  def write[T <: Product](ds: Dataset[T], tableName: TableName[T]): Unit = {
    writeDF(ds.toDF(), tableName.fullTableName())
  }

  private[spark] def resolveInputTables(inputPath: String): Seq[SosTable] = {
    val (path, params) = parsePathParams(inputPath)
    val (rawSosOptions, sparkOptions) = params.toMap.partition(_._1.toLowerCase(Locale.ROOT).startsWith(SOS_PREFIX))
    val sosOptions = toSosOptions(rawSosOptions)

    val lastPathSegment = dfs.path(path).getName
    val tnAndLsMsg = s"${SOS_PREFIX}table_name and ${SOS_PREFIX}listing_strategy"
    val inputTables = (lastPathSegment, sosOptions.tableName, sosOptions.listingStrategy) match {
      case (_, Some(_), Some(_)) =>
        throw new IllegalArgumentException(s"Having both $tnAndLsMsg at the same time are not supported ($inputPath).")
      case (SOS_LIST_PATTERN(_), Some(_), _) | (SOS_LIST_PATTERN(_), _, Some(_)) =>
        throw new IllegalArgumentException(s"Parameters $tnAndLsMsg are not supported *.list files ($inputPath).")
      case (SOS_LIST_PATTERN(_), None, None) =>
        val content = IO.using(dfs.open(path)) { is => IOUtils.toString(is, StandardCharsets.UTF_8) }
        // TODO: async, cycles, max depth
        content.split('\n').filterNot(_.startsWith("#")).flatMap(resolveInputTables).toSeq
      case (_, None, Some(strategy)) => listTables(strategy, path, sosOptions.copy(listingStrategy = None),
        sparkOptions)
      case (lastPathSegment, None, None) => Seq(SosTable(lastPathSegment, path, sosOptions.format, sparkOptions))
      case (_, Some(tableName), None) => Seq(SosTable(tableName, path, sosOptions.format, sparkOptions))
    }
    inputTables
  }

  private[spark] def listTables(strategy: String, path: String, sosOptions: SosOptions, options: Map[String, String])
  : Seq[SosTable] = {
    strategy match {
      case "dag" => resolveInputTables(SosTable(null, s"$path/output.list", sosOptions.format, options).toUrlString())
      case "tables" =>
        dfs.listStatus(path)
            .filter(s => s.isDirectory && !s.getPath.getName.startsWith("."))
            .map(s => SosTable(s.getPath.getName, s.getPath.toString, sosOptions.format, options))
      case s => throw new IllegalArgumentException(s"Unsupported listing strategy '$s'.")
      // TODO: add timestamp based strategies
    }
  }

  private[spark] def registerInputTable(inputTable: SosTable): DataFrame = {
    logger.info(s"Registering input table $inputTable")
    var reader = sparkSession.read
    inputTable.format.foreach(f => reader = reader.format(f))
    reader = reader.options(inputTable.options)
    val table = reader.load(inputTable.path)
    table.createOrReplaceTempView(inputTable.fullTableName)
    table
  }

  override def close(): Unit = {
    config.outputPath.foreach { outputPath =>
      IO.using(dfs.create(s"$outputPath/$METADATA_DIR/input_paths.list")) { dos =>
        IOUtils.write(inputPaths.mkString("", "\n", "\n"), dos)
      }
      IO.using(dfs.create(s"$outputPath/$METADATA_DIR/resolved_input_tables.list")) { dos =>
        IOUtils.write(resolvedInputTables.map(_.toUrlString()).mkString("", "\n", "\n"), dos)
      }
      IO.using(dfs.create(s"$outputPath/output.list")) { dos =>
        IOUtils.write(outputTables.map(_.toUrlString()).mkString("", "\n", "\n"), dos)
      }
    }
  }
}

object SparkIO {
  val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  val METADATA_DIR = ".meta"

  val SOS_PREFIX = "sos-"
  val SOS_LIST_PATTERN = "(?i)(.*)\\.list".r

  case class SosOptions(format: Option[String], tableName: Option[String], listingStrategy: Option[String])

  case class SosTable(fullTableName: String, path: String, format: Option[String], options: Map[String, String]) {
    def toUrlString(): String = {
      val uriBuilder = new URIBuilder(path)
      format.foreach(f => uriBuilder.addParameter(s"${SOS_PREFIX}format", f))
      Option(fullTableName).foreach(ftn => uriBuilder.addParameter(s"${SOS_PREFIX}table_name", ftn))
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

  private def toSosOptions(params: Map[String, String]): SosOptions = {
    val normalizedSosParams = params.map(kv => (kv._1.toLowerCase(Locale.ROOT).stripPrefix(SOS_PREFIX), kv._2))
    SosOptions(
      format = normalizedSosParams.get("format"),
      tableName = normalizedSosParams.get("table_name"),
      listingStrategy = normalizedSosParams.get("listing_strategy").map(_.toLowerCase(Locale.ROOT)))
  }
}

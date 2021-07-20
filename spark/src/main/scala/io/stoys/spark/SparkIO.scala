package io.stoys.spark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.net.{URI, URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.util.matching.Regex

// Note: Do not forget to call init() first to register config.inputPaths!
class SparkIO(sparkSession: SparkSession, config: SparkIOConfig) extends AutoCloseable {
  import SparkIO._

  private val logger = org.log4s.getLogger

  private val dfs = Dfs(sparkSession)

  private val inputPaths = mutable.Buffer.empty[String]
  private val inputDags = mutable.Buffer.empty[SosDag]
  private val inputTables = mutable.Map.empty[String, SosTable]
  private val outputTables = mutable.Map.empty[String, SosTable]

  def init(): Unit = {
    addInputPaths(config.inputPaths: _*)
  }

  private def addInputTable(table: SosTable): Unit = {
    inputTables.get(table.table_name) match {
      case Some(existingTable) if existingTable == table =>
        logger.debug(s"Resolved the same table again ($table).")
      case Some(existingTable) =>
        val msg = s"Resolved conflicting tables for table_name ${table.table_name} ($existingTable and $table)!"
        logger.error(msg)
        throw new SToysException(msg)
      case None =>
        inputTables.put(table.table_name, table)
        registerInputTable(table)
    }
  }

  def addInputPaths(paths: String*): Unit = {
    paths.foreach { path =>
      inputPaths.append(path)
      resolveInputs(path).foreach {
        case dag: SosDag => inputDags.append(dag)
        case table: SosTable => addInputTable(table)
      }
    }
  }

  def df[T <: Product](tableName: TableName[T]): DataFrame = {
    sparkSession.table(tableName.fullTableName())
  }

  def ds[T <: Product](tableName: TableName[T]): Dataset[T] = {
    Reshape.reshape[T](df(tableName), config.inputReshapeConfig)(tableName.typeTag)
  }

  private def writeDF(df: DataFrame, fullTableName: String,
      format: Option[String], writeMode: Option[String], options: Map[String, String]): Unit = {
    val path = s"${config.outputPath.get}/$fullTableName"
    val table = SosTable(fullTableName, path, format, options)
    if (outputTables.contains(table.table_name)) {
      val msg = s"Writing table with the same table_name ${table.table_name} multiple times!"
      logger.error(msg)
      throw new SToysException(msg)
    } else {
      logger.info(s"Writing $fullTableName to $path.")
      val writer = df.write
      table.format.foreach(writer.format)
      writeMode.foreach(writer.mode)
      writer.options(table.options)
      writer.save(path)
      outputTables.put(table.table_name, table)
    }
  }

  def write[T <: Product](ds: Dataset[T], tableName: TableName[T]): Unit = {
    write[T](ds, tableName, config.writeFormat, config.writeMode, config.writeOptions)
  }

  def write[T <: Product](ds: Dataset[T], tableName: TableName[T],
      format: Option[String], writeMode: Option[String], options: Map[String, String]): Unit = {
    writeDF(ds.toDF(), tableName.fullTableName(), format, writeMode, options)
  }

  // TODO: Add listing strategies based on timestamp, checking success, etc.
  private[spark] def resolveInputs(inputPath: String): Seq[SosInput] = {
    val ParsedInputPath(path, sosOptions, options) = parseInputPath(inputPath)
    val defaultTable = SosTable(null, path, sosOptions.format, options)

    val tnAndLsMsg = s"${SOS_PREFIX}table_name and ${SOS_PREFIX}listing_strategy"
    val inputs = (dfs.path(path).getName, sosOptions.table_name, sosOptions.listing_strategy) match {
      case (_, Some(_), Some(_)) =>
        throw new SToysException(s"Having both $tnAndLsMsg at the same time are not supported ($inputPath).")
      case (SOS_LIST_PATTERN(_), Some(_), _) | (SOS_LIST_PATTERN(_), _, Some(_)) =>
        throw new SToysException(s"Parameters $tnAndLsMsg are not supported on *.list files ($inputPath).")
      case (SOS_LIST_PATTERN(_), None, None) =>
        dfs.readString(path).split('\n').filterNot(_.startsWith("#")).filterNot(_.isEmpty).flatMap(resolveInputs).toSeq
      case (_, None, Some("tables")) =>
        val fileStatuses = dfs.listStatus(path).filter(s => s.isDirectory && !s.getPath.getName.startsWith("."))
        fileStatuses.map(s => defaultTable.copy(table_name = s.getPath.getName, path = s.getPath.toString)).toSeq
      case (_, None, Some(strategy)) if strategy.startsWith("dag") => resolveDagInputs(path, sosOptions, options)
      case (_, None, Some(strategy)) => throw new SToysException(s"Unsupported listing strategy '$strategy'.")
      case (DAG_DIR, None, None) =>
        resolveDagInputs(dfs.path(path).getParent.toString, sosOptions.copy(listing_strategy = Some("dag")), options)
      case (lastPathSegment, None, None) if !lastPathSegment.startsWith(".") =>
        Seq(defaultTable.copy(table_name = lastPathSegment))
      case (_, Some(tableName), None) => Seq(defaultTable.copy(table_name = tableName))
      case (lastPathSegment, None, None) if lastPathSegment.startsWith(".") =>
        throw new SToysException(s"Unsupported path starting with dot '$lastPathSegment'.")
    }
    inputs
  }

  private def resolveDagInputs(path: String, sosOptions: SosOptions, options: Map[String, String]): Seq[SosInput] = {
    def resolveDagList(path: String): Seq[SosInput] = {
      resolveInputs(SosTable(null, path, sosOptions.format, options).toUrlString)
    }

    lazy val inputTables = resolveDagList(s"$path/$DAG_DIR/input_tables.list")
    lazy val outputTables = resolveDagList(s"$path/$DAG_DIR/output_tables.list")
    lazy val inputDags = resolveDagList(s"$path/$DAG_DIR/input_dags.list")
    val inputs = sosOptions.listing_strategy match {
      case Some("dag") => outputTables
      case Some("dag_io") => outputTables ++ inputTables
      case Some("dag_io_recursive") =>
        // TODO: async, cycles, max depth
        val upstreamInputs = inputDags.collect {
          case dag: SosDag => resolveDagInputs(dag.path, sosOptions, options)
        }
        outputTables ++ inputTables ++ upstreamInputs.flatten
      case strategy => throw new SToysException(s"Unsupported dag listing strategy '$strategy'.")
    }
    Seq(SosDag(path)) ++ inputs
  }

  private[spark] def registerInputTable(inputTable: SosTable): DataFrame = {
    logger.info(s"Registering input table $inputTable")
    var reader = sparkSession.read
    inputTable.format.foreach(f => reader = reader.format(f))
    reader = reader.options(inputTable.options)
    val table = reader.load(inputTable.path)
    table.createOrReplaceTempView(inputTable.table_name)
    table
  }

  override def close(): Unit = {
    config.outputPath.foreach { outputPath =>
      dfs.writeString(s"$outputPath/$DAG_DIR/input_paths.list", inputPaths.mkString("\n"))
      dfs.writeString(s"$outputPath/$DAG_DIR/input_dags.list", inputDags.map(_.toUrlString).mkString("\n"))
      val inputTablesContent = inputTables.map(_._2.toUrlString).toSeq.sorted.mkString("\n")
      dfs.writeString(s"$outputPath/$DAG_DIR/input_tables.list", inputTablesContent)
      val outputTablesContent = outputTables.map(_._2.toUrlString).toSeq.sorted.mkString("\n")
      dfs.writeString(s"$outputPath/$DAG_DIR/output_tables.list", outputTablesContent)
    }
  }

  def getInputTable(fullTableName: String): Option[SosTable] = {
    inputTables.get(fullTableName)
  }

  def writeSymLink(path: String): Unit = {
    config.outputPath.foreach(outputPath => dfs.writeString(path, SosDag(outputPath).toUrlString))
  }
}

object SparkIO {
  val DAG_DIR = ".dag"
  val SOS_PREFIX = "sos-"
  val SOS_LIST_PATTERN: Regex = "(?i)(.*)\\.list".r

  case class SosOptions(
      table_name: Option[String],
      format: Option[String],
      listing_strategy: Option[String]
  )

  sealed trait SosInput {
    def toUrlString: String
  }

  case class SosTable(
      table_name: String,
      path: String,
      format: Option[String],
      options: Map[String, String]
  ) extends SosInput {
    override def toUrlString: String = {
      val tableNameParam = Option(table_name).map(n => s"${SOS_PREFIX}table_name" -> n)
      val formatParam = format.map(f => s"${SOS_PREFIX}format" -> f).toSeq
      val optionParams = options.toSeq
      val allParams = tableNameParam ++ formatParam ++ optionParams
      val allEncodedParams = allParams.map {
        case (key, null) => key
        case (key, value) => s"$key=${URLEncoder.encode(value, StandardCharsets.UTF_8.toString)}"
      }
      allEncodedParams.mkString(s"$path?", "&", "")
    }
  }

  case class SosDag(
      path: String
  ) extends SosInput {
    override def toUrlString: String = {
      s"$path?sos-listing_strategy=dag"
    }
  }

  case class ParsedInputPath(
      path: String,
      sosOptions: SosOptions,
      options: Map[String, String]
  )

  private[stoys] def parseInputPath(inputPath: String): ParsedInputPath = {
    val pathParamsUri = new URI(inputPath)
    val path = pathParamsUri.getPath
    val params = Option(pathParamsUri.getRawQuery).toSeq.flatMap(_.split("&").map { rawParam =>
      rawParam.split('=') match {
        case Array(key, rawValue) => key -> URLDecoder.decode(rawValue, StandardCharsets.UTF_8.toString)
        case Array(key) if rawParam.length > key.length => key -> ""
        case Array(key) => key -> null
      }
    })
    val (rawSosOptions, options) = params.toMap.partition(_._1.toLowerCase.startsWith(SOS_PREFIX))
    val sosOptions = toSosOptions(rawSosOptions)
    ParsedInputPath(path, sosOptions, options)
  }

  private def toSosOptions(params: Map[String, String]): SosOptions = {
    val normalizedSosParams = params.map(kv => (kv._1.toLowerCase.stripPrefix(SOS_PREFIX), kv._2))
    SosOptions(
      table_name = normalizedSosParams.get("table_name"),
      format = normalizedSosParams.get("format"),
      listing_strategy = normalizedSosParams.get("listing_strategy").map(_.toLowerCase))
  }
}

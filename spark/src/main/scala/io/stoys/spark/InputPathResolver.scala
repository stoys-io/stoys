package io.stoys.spark

import com.fasterxml.jackson.annotation.JsonProperty
import io.stoys.scala.{Arbitrary, Configuration, Jackson, Strings}

import java.net.{URI, URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

private[spark] class InputPathResolver(dfs: Dfs) {
  import InputPathResolver._

  // TODO: Add listing strategies based on timestamp, checking success, etc.
  def resolveInputs(inputPath: String): Seq[Info] = {
    val tnAndLsMsg = s"${STOYS_OPTION_PREFIX}table_name and ${STOYS_OPTION_PREFIX}listing_strategy"
    val parsedInputPath = parseInputPath(inputPath)
    val path = parsedInputPath.path
    val fileName = dfs.path(path).getName
    val tableName = parsedInputPath.stoysOptions.table_name
    val listingStrategy = parsedInputPath.stoysOptions.listing_strategy.map(_.toLowerCase)
    val defaultTableInfo = TableInfo(parsedInputPath.path, tableName = null, parsedInputPath.stoysOptions.format,
      parsedInputPath.stoysOptions.reshape_config_props_overrides, parsedInputPath.sparkOptions)
    val inputs = (fileName, tableName, listingStrategy) match {
      case (_, Some(_), Some(_)) =>
        throw new SToysException(s"Having both $tnAndLsMsg at the same time are not supported ($inputPath).")
      case (STOYS_LIST_PATTERN(_), Some(_), _) | (STOYS_LIST_PATTERN(_), _, Some(_)) =>
        throw new SToysException(s"Parameters $tnAndLsMsg are not supported on *.list files ($inputPath).")
      case (STOYS_LIST_PATTERN(_), None, None) =>
        dfs.readString(path).split('\n').filterNot(_.startsWith("#")).filterNot(_.isEmpty).flatMap(resolveInputs).toSeq
      case (_, None, Some("tables")) =>
        val fileStatuses = dfs.listStatus(path).filter(s => s.isDirectory && !s.getPath.getName.startsWith("."))
        fileStatuses.map(s => defaultTableInfo.copy(tableName = s.getPath.getName, path = s.getPath.toString)).toSeq
      case (_, None, Some(strategy)) if strategy.startsWith("dag") =>
        resolveDagInputs(path, listingStrategy)
      case (_, None, Some(strategy)) =>
        throw new SToysException(s"Unsupported listing strategy '$strategy'.")
      case (DAG_DIR, None, None) =>
        resolveDagInputs(dfs.path(path).getParent.toString, Some("dag"))
      case (_, Some(tableName), None) =>
        Seq(defaultTableInfo.copy(tableName = tableName))
      case (lastPathSegment, None, None) if !lastPathSegment.startsWith(".") =>
        Seq(defaultTableInfo.copy(tableName = Strings.toWordCharacters(lastPathSegment)))
      case (lastPathSegment, None, None) if lastPathSegment.startsWith(".") =>
        throw new SToysException(s"Unsupported path starting with dot '$lastPathSegment'.")
    }
    inputs
  }

  private def resolveDagInputs(path: String, listingStrategy: Option[String]): Seq[Info] = {
    lazy val inputTables = resolveInputs(s"$path/$DAG_DIR/input_tables.list")
    lazy val outputTables = resolveInputs(s"$path/$DAG_DIR/output_tables.list")
    lazy val inputDags = resolveInputs(s"$path/$DAG_DIR/input_dags.list")
    val inputs = listingStrategy.map(_.toLowerCase) match {
      case Some("dag") => outputTables
      case Some("dag_io") => outputTables ++ inputTables
      case Some("dag_io_recursive") =>
        // TODO: async, cycles, max depth
        val upstreamInputs = inputDags.collect {
          case dagInfo: DagInfo =>
            val recursiveInputPath = s"${dagInfo.path}?${STOYS_OPTION_PREFIX}listing_strategy=dag_io_recursive"
            outputTables ++ inputTables ++ resolveInputs(recursiveInputPath)
        }
        outputTables ++ inputTables ++ upstreamInputs.flatten
      case strategy => throw new SToysException(s"Unsupported dag listing strategy '$strategy'.")
    }
    Seq(DagInfo(path)) ++ inputs
  }
}

object InputPathResolver {
  private[spark] val DAG_DIR = ".dag"
  private val STOYS_OPTION_PREFIX = "sos-" // TODO: change to "stoys."?
  private val STOYS_LIST_PATTERN = "^(?i)(.*)\\.list$".r("name")

  sealed trait Info {
    def toInputPathUrl: String
  }

  case class TableInfo(
      path: String,
      tableName: String,
      format: Option[String],
      reshapeConfigPropsOverrides: Map[String, String],
      sparkOptions: Map[String, String]
  ) extends Info {
    override def toInputPathUrl: String = {
      val stoysOptions = StoysOptions(Option(tableName), format, listing_strategy = None, reshapeConfigPropsOverrides)
      val stoysProps = Configuration.propsWriter.writeValueAsString(stoysOptions)
      val prefixedStoysOptions = stoysProps.split("\n").map(_.split('=')).collect {
        case Array(key, value) => s"$STOYS_OPTION_PREFIX$key" -> value
      }
      val encodedOptionFragments = (prefixedStoysOptions ++ sparkOptions).toSeq.sorted.map {
        case (key, null) => urlEncode(key)
        case (key, value) => s"${urlEncode(key)}=${urlEncode(value)}"
      }
      if (encodedOptionFragments.isEmpty) {
        path
      } else {
        s"$path?${encodedOptionFragments.mkString("&")}"
      }
    }
  }

  case class DagInfo(
      path: String
  ) extends Info {
    override def toInputPathUrl: String = {
      s"$path?${STOYS_OPTION_PREFIX}listing_strategy=dag"
    }
  }

  private[spark] case class StoysOptions(
      table_name: Option[String],
      format: Option[String],
      listing_strategy: Option[String],
      @JsonProperty("reshape_config")
      reshape_config_props_overrides: Map[String, String]
  )

  private[spark] case class ParsedInputPath(
      path: String,
      stoysOptions: StoysOptions,
      sparkOptions: Map[String, String]
  )

  private[spark] def parseInputPath(inputPath: String): ParsedInputPath = {
    val pathParamsUri = new URI(inputPath)
    val options = Option(pathParamsUri.getRawQuery).toSeq.flatMap(_.split('&').map { rawParam =>
      rawParam.split('=') match {
        case Array(rawKey, rawValue) => urlDecode(rawKey) -> urlDecode(rawValue)
        case Array(rawKey) if rawParam.length > rawKey.length => urlDecode(rawKey) -> ""
        case Array(rawKey) => urlDecode(rawKey) -> null
      }
    })
    val (rawStoysOptions, sparkOptions) = options.toMap.partition(_._1.toLowerCase.startsWith(STOYS_OPTION_PREFIX))
    val rawStoysOptionsPropsString =
      rawStoysOptions.map(kv => s"${kv._1.toLowerCase.stripPrefix(STOYS_OPTION_PREFIX)}=${kv._2}").mkString("\n")
    val rawStoysOptionsJsonNode = Configuration.propsReader.readTree(rawStoysOptionsPropsString)
    val stoysOptions = Jackson.json.updateValue(Arbitrary.empty[StoysOptions], rawStoysOptionsJsonNode)
    ParsedInputPath(pathParamsUri.getPath, stoysOptions, sparkOptions)
  }

  private def urlEncode(value: String): String = {
    URLEncoder.encode(value, StandardCharsets.UTF_8.toString)
  }

  private def urlDecode(value: String): String = {
    URLDecoder.decode(value, StandardCharsets.UTF_8.toString)
  }
}

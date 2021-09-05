package io.stoys.spark

import com.fasterxml.jackson.annotation.JsonProperty
import io.stoys.scala.{Arbitrary, Configuration, Jackson, Strings}

import java.net.{URI, URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

private[spark] class InputPathResolver(dfs: Dfs) {
  import InputPathResolver._

  // TODO: Add listing strategies based on timestamp, checking success, etc.
  def resolveInputs(inputPath: String): Seq[SosInput] = {
    val ParsedInputPath(path, sosOptions, options) = parseInputPath(inputPath)
    val defaultSosTable = SosTable(path, null, sosOptions.format, sosOptions.reshape_config_props_overrides, options)

    val tnAndLsMsg = s"${SOS_OPTION_PREFIX}table_name and ${SOS_OPTION_PREFIX}listing_strategy"
    val inputs = (dfs.path(path).getName, sosOptions.table_name, sosOptions.listing_strategy.map(_.toLowerCase)) match {
      case (_, Some(_), Some(_)) =>
        throw new SToysException(s"Having both $tnAndLsMsg at the same time are not supported ($inputPath).")
      case (SOS_LIST_PATTERN(_), Some(_), _) | (SOS_LIST_PATTERN(_), _, Some(_)) =>
        throw new SToysException(s"Parameters $tnAndLsMsg are not supported on *.list files ($inputPath).")
      case (SOS_LIST_PATTERN(_), None, None) =>
        dfs.readString(path).split('\n').filterNot(_.startsWith("#")).filterNot(_.isEmpty).flatMap(resolveInputs).toSeq
      case (_, None, Some("tables")) =>
        val fileStatuses = dfs.listStatus(path).filter(s => s.isDirectory && !s.getPath.getName.startsWith("."))
        fileStatuses.map(s => defaultSosTable.copy(table_name = s.getPath.getName, path = s.getPath.toString)).toSeq
      case (_, None, Some(strategy)) if strategy.startsWith("dag") => resolveDagInputs(path, sosOptions, options)
      case (_, None, Some(strategy)) => throw new SToysException(s"Unsupported listing strategy '$strategy'.")
      case (DAG_DIR, None, None) =>
        resolveDagInputs(dfs.path(path).getParent.toString, sosOptions.copy(listing_strategy = Some("dag")), options)
      case (_, Some(tableName), None) => Seq(defaultSosTable.copy(table_name = tableName))
      case (lastPathSegment, None, None) if !lastPathSegment.startsWith(".") =>
        Seq(defaultSosTable.copy(table_name = Strings.toWordCharacters(lastPathSegment)))
      case (lastPathSegment, None, None) if lastPathSegment.startsWith(".") =>
        throw new SToysException(s"Unsupported path starting with dot '$lastPathSegment'.")
    }
    inputs
  }

  private def resolveDagInputs(path: String, sosOptions: SosOptions, options: Map[String, String]): Seq[SosInput] = {
    def resolveDagList(path: String): Seq[SosInput] = {
      val sosTable = SosTable(path, null, sosOptions.format, sosOptions.reshape_config_props_overrides, options)
      resolveInputs(sosTable.toUrlString)
    }

    lazy val inputTables = resolveDagList(s"$path/$DAG_DIR/input_tables.list")
    lazy val outputTables = resolveDagList(s"$path/$DAG_DIR/output_tables.list")
    lazy val inputDags = resolveDagList(s"$path/$DAG_DIR/input_dags.list")
    val inputs = sosOptions.listing_strategy.map(_.toLowerCase) match {
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
}

object InputPathResolver {
  private[spark] val DAG_DIR = ".dag"
  private val SOS_OPTION_PREFIX = "sos-" // TODO: change to "stoys."?
  private val SOS_LIST_PATTERN = "^(?i)(.*)\\.list$".r("name")

  sealed trait SosInput {
    def toUrlString: String
  }

  case class SosTable(
      path: String,
      table_name: String,
      format: Option[String],
      reshape_config_props_overrides: Map[String, String],
      options: Map[String, String]
  ) extends SosInput {
    override def toUrlString: String = {
      val sosOptions = SosOptions(Option(table_name), format, listing_strategy = None, reshape_config_props_overrides)
      val sosParams = Configuration.propsWriter.writeValueAsString(sosOptions).split("\n").map(_.split('=')).collect {
        case Array(key, value) => s"$SOS_OPTION_PREFIX$key" -> value
      }
      val allEncodedParams = (sosParams ++ options).toSeq.sorted.map {
        case (key, null) => urlEncode(key)
        case (key, value) => s"${urlEncode(key)}=${urlEncode(value)}"
      }
      if (allEncodedParams.isEmpty) {
        path
      } else {
        s"$path?${allEncodedParams.mkString("&")}"
      }
    }
  }

  case class SosDag(
      path: String
  ) extends SosInput {
    override def toUrlString: String = {
      s"$path?${SOS_OPTION_PREFIX}listing_strategy=dag"
    }
  }

  private[spark] case class SosOptions(
      table_name: Option[String],
      format: Option[String],
      listing_strategy: Option[String],
      @JsonProperty("reshape_config")
      reshape_config_props_overrides: Map[String, String]
  )

  private[spark] case class ParsedInputPath(
      path: String,
      sosOptions: SosOptions,
      options: Map[String, String]
  )

  private[spark] def parseInputPath(inputPath: String): ParsedInputPath = {
    val pathParamsUri = new URI(inputPath)
    val path = pathParamsUri.getPath
    val params = Option(pathParamsUri.getRawQuery).toSeq.flatMap(_.split('&').map { rawParam =>
      rawParam.split('=') match {
        case Array(rawKey, rawValue) => urlDecode(rawKey) -> urlDecode(rawValue)
        case Array(rawKey) if rawParam.length > rawKey.length => urlDecode(rawKey) -> ""
        case Array(rawKey) => urlDecode(rawKey) -> null
      }
    })
    val (rawSosOptions, options) = params.toMap.partition(_._1.toLowerCase.startsWith(SOS_OPTION_PREFIX))
    val sosOptions = toSosOptions(rawSosOptions)
    ParsedInputPath(path, sosOptions, options)
  }

  private def toSosOptions(params: Map[String, String]): SosOptions = {
    val normalizedSosParams = params.map(kv => (kv._1.toLowerCase.stripPrefix(SOS_OPTION_PREFIX), kv._2))
    val normalizedSosParamsString = normalizedSosParams.map(kv => s"${kv._1}=${kv._2}").mkString("\n")
    Jackson.json.updateValue(Arbitrary.empty[SosOptions], Configuration.propsReader.readTree(normalizedSosParamsString))
  }

  private def urlEncode(value: String): String = {
    URLEncoder.encode(value, StandardCharsets.UTF_8.toString)
  }

  private def urlDecode(value: String): String = {
    URLDecoder.decode(value, StandardCharsets.UTF_8.toString)
  }
}

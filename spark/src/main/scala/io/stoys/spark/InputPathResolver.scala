package io.stoys.spark

import io.stoys.scala.Strings

import java.net.{URI, URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

private[spark] class InputPathResolver(dfs: Dfs) {
  import InputPathResolver._

  // TODO: Add listing strategies based on timestamp, checking success, etc.
  def resolveInputs(inputPath: String): Seq[SosInput] = {
    val ParsedInputPath(path, sosOptions, options) = parseInputPath(inputPath)
    val defaultTable = SosTable(null, path, sosOptions.format, options)

    val tnAndLsMsg = s"${SOS_OPTION_PREFIX}table_name and ${SOS_OPTION_PREFIX}listing_strategy"
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
      case (_, Some(tableName), None) => Seq(defaultTable.copy(table_name = tableName))
      case (lastPathSegment, None, None) if !lastPathSegment.startsWith(".") =>
        Seq(defaultTable.copy(table_name = Strings.toWordCharacters(lastPathSegment)))
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
}

object InputPathResolver {
  private[spark] val DAG_DIR = ".dag"
  private val SOS_OPTION_PREFIX = "sos-"
  private val SOS_LIST_PATTERN = "^(?i)(.*)\\.list$".r("name")

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
      val tableNameParam = Option(table_name).map(n => s"${SOS_OPTION_PREFIX}table_name" -> n)
      val formatParam = format.map(f => s"${SOS_OPTION_PREFIX}format" -> f).toSeq
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
      s"$path?${SOS_OPTION_PREFIX}listing_strategy=dag"
    }
  }

  private[spark] case class SosOptions(
      table_name: Option[String],
      format: Option[String],
      listing_strategy: Option[String]
  )

  private[spark] case class ParsedInputPath(
      path: String,
      sosOptions: SosOptions,
      options: Map[String, String]
  )

  private[spark] def parseInputPath(inputPath: String): ParsedInputPath = {
    val pathParamsUri = new URI(inputPath)
    val path = pathParamsUri.getPath
    val params = Option(pathParamsUri.getRawQuery).toSeq.flatMap(_.split("&").map { rawParam =>
      rawParam.split('=') match {
        case Array(key, rawValue) => key -> URLDecoder.decode(rawValue, StandardCharsets.UTF_8.toString)
        case Array(key) if rawParam.length > key.length => key -> ""
        case Array(key) => key -> null
      }
    })
    val (rawSosOptions, options) = params.toMap.partition(_._1.toLowerCase.startsWith(SOS_OPTION_PREFIX))
    val sosOptions = toSosOptions(rawSosOptions)
    ParsedInputPath(path, sosOptions, options)
  }

  private def toSosOptions(params: Map[String, String]): SosOptions = {
    val normalizedSosParams = params.map(kv => (kv._1.toLowerCase.stripPrefix(SOS_OPTION_PREFIX), kv._2))
    SosOptions(
      table_name = normalizedSosParams.get("table_name"),
      format = normalizedSosParams.get("format"),
      listing_strategy = normalizedSosParams.get("listing_strategy").map(_.toLowerCase))
  }
}

package com.nuna.trustdb.core.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectReader}
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema
import com.nuna.trustdb.core.util.Jackson.{objectMapper, propertiesObjectMapper, yamlObjectMapper}

import scala.reflect.runtime.universe._
import scala.reflect.{ClassTag, classTag}

// Configuration.
//
// parseConfigurationConfig - called when you construct this an instance of this class it will call.
// That will extract environments from comma separated environments in flag "--environments=".
// It will automatically append "local" environment to the end (highest precedence).
//
// readConfig[SomeType]() will return instance of T. It will first create empty instance (0 for primitives,
// empty for collections and null otherwise). Then it will start updating the instance by default
// config file some_type.yaml (some_type = snake cased SomeType), followed by some_type-ent.yaml (where env
// is environment in order specified). Finally the instance get updated by command line arguments
// (highest priority). Command line arguments are treated as property files prefixed by snake_cased_class_name.
//
// Note: Command line argument path separator is "@@" instead of usual "." because sparkOptions
//       have all the keys with "." and that would confuse the parser.
//
// Note: Do not use nested config classes. (your collections would become null instead of empty)
//
// Note: repeated properties are not comma separated. Instead they use "field@@0=foo" "field@@1=bar" notation.
// Note: This class is inspired by spring boot config. But that sadly does not work for scala case classes.
// Note: We can trivially add support for *.properties and *.json.
class Configuration(args: Array[String]) {
  private val logger = org.log4s.getLogger

  import Configuration._

  val config = parseConfigurationConfig(args)

  def readConfig[T <: Product : ClassTag : TypeTag]: T = {
    val clazz = classTag[T].runtimeClass
    val baseFileName = Strings.toSnakeCase(Reflection.typeNameOf[T])
    var value = Arbitrary.empty[T]

    val params = Reflection.getAnnotationParams[Params](Reflection.dealiasedTypeSymbolOf[T]).getOrElse(Seq.empty).toMap
    val allowInRootPackage = params.getOrElse("allowInRootPackage", false).asInstanceOf[Boolean]

    // files based overrides
    val candidateFileNames = Seq(s"$baseFileName.yaml") ++ config.environments.map(e => s"$baseFileName.$e.yaml")
    val rootPackageCandidateFileNames = if (allowInRootPackage) {
      candidateFileNames.map(cfn => "/" + cfn)
    } else {
      Seq.empty
    }
    val allCandidateFileNames = candidateFileNames ++ rootPackageCandidateFileNames
    allCandidateFileNames.foreach { fileName =>
      value = update[T](value, IO.safeReadResource(clazz, fileName), yamlReader, fileName, None)
    }

    // master_config file based overrides
    val masterCandidateFileNames = Seq("/master_config.yaml") ++ config.environments.map(e => s"/master_config.$e.yaml")
    masterCandidateFileNames.foreach { fileName =>
      value = update[T](value, IO.safeReadResource(clazz, fileName), yamlReader, fileName, Some(baseFileName))
    }

    // command line overrides
    val argsConfigurationString = Option(config.args.mkString("\n"))
    update[T](value, argsConfigurationString, propertiesReader, "'props' style command line flags", Some(baseFileName))
  }

  private def update[T <: Product : ClassTag : TypeTag](originalValue: T, configurationString: Option[String],
      objectReader: ObjectReader, logMessage: String, rootFieldName: Option[String]): T = {
    val logMessagePrefix = s"Configuring ${classTag[T].runtimeClass.getName}: $logMessage"
    configurationString match {
      case Some(configString) =>
        val jsonTree = objectReader.readTree(configString)
        rootFieldName match {
          case None =>
            logger.info(s"$logMessagePrefix overwritten ${prettyPrintJsonNode[T](jsonTree)}")
            objectMapper.reader().withValueToUpdate(originalValue).readValue[T](jsonTree)
          case Some(fieldName) if jsonTree.has(fieldName) =>
            logger.info(s"$logMessagePrefix overwritten ${prettyPrintJsonNode[T](jsonTree.get(fieldName))}")
            objectMapper.reader().withValueToUpdate(originalValue).readValue[T](jsonTree.get(fieldName))
          case Some(fieldName) =>
            logger.debug(s"$logMessagePrefix have no field $rootFieldName.")
            originalValue
        }
      case None =>
        logger.debug(s"$logMessagePrefix not present.")
        originalValue
    }
  }

  private def prettyPrintJsonNode[T <: Product : ClassTag : TypeTag](jsonNode: JsonNode): String = {
    val overrides = objectReader.withValueToUpdate(Arbitrary.empty[T]).readValue[T](jsonNode)
    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(overrides)
  }
}

object Configuration {
  private val ENVIRONMENT_ARGS_PREFIX = "--environments="
  private val DEFAULT_ENVIRONMENTS = Seq("local")
  private val PROPERTIES_PATH_SEPARATOR = "@@"

  val objectReader = objectMapper.reader()
  val javaPropsSchema = JavaPropsSchema.emptySchema().withPathSeparator(PROPERTIES_PATH_SEPARATOR)
  val propertiesReader = propertiesObjectMapper.reader(javaPropsSchema)
  val yamlReader = yamlObjectMapper.reader()

  case class ConfigurationConfig(environments: Seq[String], args: Seq[String])

  def parseConfigurationConfig(args: Array[String]): ConfigurationConfig = {
    val (environmentArgs, remainingArgs) = args.partition(_.startsWith(ENVIRONMENT_ARGS_PREFIX))
    val environments = environmentArgs.flatMap(_.substring(ENVIRONMENT_ARGS_PREFIX.size).split(',')).toSeq
    ConfigurationConfig(environments ++ DEFAULT_ENVIRONMENTS, remainingArgs)
  }
}

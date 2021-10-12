package io.stoys.scala

import com.fasterxml.jackson.databind.{JsonNode, ObjectReader}
import com.fasterxml.jackson.dataformat.javaprop.{JavaPropsMapper, JavaPropsSchema}
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import io.stoys.scala.Configuration.ConfigurationConfig

import scala.reflect.runtime.universe.TypeTag
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
// Note: Command line argument path separator is "__" instead of usual "." because sparkOptions
//       have all the keys with "." and that would confuse the parser.
//
// Note: Do not use nested config classes. (your collections would become null instead of empty)
//
// Note: repeated properties are not comma separated. Instead they use "field__0=foo" "field__1=bar" notation.
// Note: This class is inspired by spring boot config. But that sadly does not work for scala case classes.
// Note: We can trivially add support for *.properties and *.json.
case class Configuration(config: ConfigurationConfig) {
  private val logger = org.log4s.getLogger

  import Configuration._

  def readConfig[T <: Product : ClassTag : TypeTag]: T = {
    val clazz = classTag[T].runtimeClass
    val baseFileName = Strings.toSnakeCase(clazz.getSimpleName)
    val allowInRootPackage = clazz.getAnnotationsByType(classOf[Params]).exists(_.allowInRootPackage())

    var value = Arbitrary.empty[T]

    // files based overrides
    val candidateFileNames = Seq(s"$baseFileName.yaml") ++ config.environments.map(e => s"$baseFileName.$e.yaml")
    val rootPackageCandidateFileNames = candidateFileNames.filter(_ => allowInRootPackage).map(cfn => "/" + cfn)
    val allCandidateFileNames = candidateFileNames ++ rootPackageCandidateFileNames
    allCandidateFileNames.foreach { fileName =>
      value = update[T](value, IO.resourceToStringOption(clazz, fileName), yamlReader, fileName, None)
    }

    // main_config file based overrides
    val mainCandidateFileNames = Seq("/main_config.yaml") ++ config.environments.map(e => s"/main_config.$e.yaml")
    mainCandidateFileNames.foreach { fileName =>
      value = update[T](value, IO.resourceToStringOption(clazz, fileName), yamlReader, fileName, Some(baseFileName))
    }

    // command line overrides
    val argsConfigurationString = Option(config.args.mkString("\n"))
    update[T](value, argsConfigurationString, propsReader, "'props' style command line flags", Some(baseFileName))
  }

  private def update[T <: Product : ClassTag](originalValue: T, configurationString: Option[String],
      objectReader: ObjectReader, logMessage: String, rootFieldName: Option[String]): T = {
    val logMessagePrefix = s"Configuring ${classTag[T].runtimeClass.getName}: $logMessage"
    configurationString match {
      case Some(configString) =>
        val overridesJsonNode = objectReader.readTree(configString)
        rootFieldName match {
          case None =>
            logger.info(s"$logMessagePrefix overwritten ${prettyPrintJsonNode[T](overridesJsonNode)}")
            Jackson.json.updateValue(Jackson.deepCopy(originalValue), overridesJsonNode)
          case Some(fieldName) if overridesJsonNode.has(fieldName) =>
            logger.info(s"$logMessagePrefix overwritten ${prettyPrintJsonNode[T](overridesJsonNode.get(fieldName))}")
            Jackson.json.updateValue(Jackson.deepCopy(originalValue), overridesJsonNode.get(fieldName))
          case Some(fieldName) =>
            logger.debug(s"$logMessagePrefix have no field $fieldName.")
            originalValue
        }
      case None =>
        logger.debug(s"$logMessagePrefix not present.")
        originalValue
    }
  }
}

object Configuration {
  private val ENVIRONMENT_ARGS_PREFIX = "--environments="
  private val DEFAULT_ENVIRONMENTS = Seq("local")
  private val PROPERTIES_PATH_SEPARATOR = "__"

  private val javaPropsMapper = Jackson.setMapperBuilderDefaults(JavaPropsMapper.builder()).build()
  private val javaPropsSchema = JavaPropsSchema.emptySchema().withPathSeparator(PROPERTIES_PATH_SEPARATOR)
  private[stoys] val propsReader = javaPropsMapper.reader(javaPropsSchema)
  private[stoys] val propsWriter = javaPropsMapper.writer(javaPropsSchema)
  private[stoys] val yamlReader = Jackson.setMapperBuilderDefaults(YAMLMapper.builder()).build().reader()

  def apply(args: String*): Configuration = {
    apply(args.toArray)
  }

  def apply(args: Array[String]): Configuration = {
    Configuration(parseConfigurationConfig(args))
  }

  case class ConfigurationConfig(
      environments: Seq[String],
      args: Seq[String],
  )

  def parseConfigurationConfig(args: Array[String]): ConfigurationConfig = {
    val (environmentArgs, remainingArgs) = args.partition(_.toLowerCase.startsWith(ENVIRONMENT_ARGS_PREFIX))
    val environments = environmentArgs.flatMap(_.toLowerCase.stripPrefix(ENVIRONMENT_ARGS_PREFIX).split(','))
    ConfigurationConfig(environments.toSeq ++ DEFAULT_ENVIRONMENTS, remainingArgs.toSeq)
  }

  private[scala] def prettyPrintJsonNode[T <: Product : ClassTag](jsonNode: JsonNode): String = {
    Jackson.json.writerWithDefaultPrettyPrinter().writeValueAsString(Jackson.json.treeToValue[T](jsonNode))
  }
}

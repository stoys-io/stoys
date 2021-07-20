package io.stoys.scala

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, ValueNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectReader}
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema
import io.stoys.scala.Configuration.ConfigurationConfig
import io.stoys.scala.Jackson.objectMapper

import scala.jdk.CollectionConverters._
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
    val baseFileName = Strings.toSnakeCase(Reflection.typeNameOf[T])
    var value = Arbitrary.empty[T]

    val params = Reflection.getAnnotationParams[T, Params].getOrElse(Seq.empty).toMap
    val allowInRootPackage = params.getOrElse("allowInRootPackage", false).asInstanceOf[Boolean]

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
    update[T](value, argsConfigurationString, propertiesReader, "'props' style command line flags", Some(baseFileName))
  }

  private def update[T <: Product : ClassTag : TypeTag](originalValue: T, configurationString: Option[String],
      objectReader: ObjectReader, logMessage: String, rootFieldName: Option[String]): T = {
    val logMessagePrefix = s"Configuring ${classTag[T].runtimeClass.getName}: $logMessage"
    configurationString match {
      case Some(configString) =>
        val originalTree = objectMapper.valueToTree[JsonNode](originalValue)
        val jsonTree = objectReader.readTree(configString)
        rootFieldName match {
          case None =>
            logger.info(s"$logMessagePrefix overwritten ${prettyPrintJsonNode[T](jsonTree)}")
            objectMapper.treeToValue[T](mergeJsonTrees(originalTree, jsonTree))
          case Some(fieldName) if jsonTree.has(fieldName) =>
            logger.info(s"$logMessagePrefix overwritten ${prettyPrintJsonNode[T](jsonTree.get(fieldName))}")
            objectMapper.treeToValue[T](mergeJsonTrees(originalTree, jsonTree.get(fieldName)))
          case Some(_) =>
            logger.debug(s"$logMessagePrefix have no field $rootFieldName.")
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

  private val objectReader = Jackson.objectMapper.reader()
  private val javaPropsSchema = JavaPropsSchema.emptySchema().withPathSeparator(PROPERTIES_PATH_SEPARATOR)
  private val propertiesReader = Jackson.propertiesObjectMapper.reader(javaPropsSchema)
  private val yamlReader = Jackson.yamlObjectMapper.reader()

  def apply(args: String*): Configuration = {
    apply(args.toArray)
  }

  def apply(args: Array[String]): Configuration = {
    Configuration(parseConfigurationConfig(args))
  }

  case class ConfigurationConfig(
      environments: Seq[String],
      args: Seq[String]
  )

  def parseConfigurationConfig(args: Array[String]): ConfigurationConfig = {
    val (environmentArgs, remainingArgs) = args.partition(_.toLowerCase.startsWith(ENVIRONMENT_ARGS_PREFIX))
    val environments = environmentArgs.flatMap(_.toLowerCase.stripPrefix(ENVIRONMENT_ARGS_PREFIX).split(','))
    ConfigurationConfig(environments.toSeq ++ DEFAULT_ENVIRONMENTS, remainingArgs.toSeq)
  }

  // Note: Jackson has deep merge since 2.9. It does not work (see ConfigurationTest). Once fixed following should work:
  // val om = Jackson.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT).setDefaultMergeable(true)
  // om.updateValue(original, om.writeValueAsString(update))
  //
  // BEWARE: Our implementation does not work well for maps. It verge map values! We probably don't want that.
  private[scala] def mergeJsonTrees(originalNode: JsonNode, updateNode: JsonNode): JsonNode = {
    if (updateNode == null) {
      originalNode.deepCopy()
    } else if (originalNode == null) {
      updateNode.deepCopy()
    } else {
      val originalFields = originalNode.fields().asScala.map(e => e.getKey -> e.getValue).toMap
      val updateFields = updateNode.fields().asScala.map(e => e.getKey -> e.getValue).toMap
      val fieldNames = (originalFields.keys ++ updateFields.keys).toSeq.distinct.sorted
      val result = new ObjectNode(null)
      fieldNames.foreach { name =>
        val value = (originalFields.get(name), updateFields.get(name)) match {
          case (Some(original), None) => original.deepCopy()
          case (None, Some(update)) => update.deepCopy()
          case (Some(original: ArrayNode), Some(update: ArrayNode)) => original.deepCopy().addAll(update.deepCopy())
          case (Some(original: ObjectNode), Some(update: ObjectNode)) => mergeJsonTrees(original, update)
          case (Some(_: ValueNode), Some(update: ValueNode)) => update.deepCopy()
          case (original, update) => throw new MatchError(s"Error. Unexpected json nodes: '${(original, update)}''")
        }
        result.replace(name, value)
      }
      result
    }
  }

  private[scala] def prettyPrintJsonNode[T <: Product : TypeTag](jsonNode: JsonNode): String = {
    val overrides = objectReader.withValueToUpdate(Arbitrary.empty[T]).readValue[T](jsonNode)
    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(overrides)
  }

  def updateCaseClassWithConfigMap[T <: Product : TypeTag](originalValue: T, configMap: Map[String, Any]): T = {
    var value = Arbitrary.empty[T]
    val originalValueTree = objectMapper.valueToTree[JsonNode](originalValue)
    value = objectMapper.reader().withValueToUpdate(value).readValue[T](originalValueTree)
    val configMapTree = objectMapper.readTree(objectMapper.writeValueAsString(configMap))
    value = objectMapper.reader().withValueToUpdate(value).readValue[T](configMapTree)
    value
  }
}

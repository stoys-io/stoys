package io.stoys.scala

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.cfg.MapperBuilder
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.ClassTagExtensions

import scala.reflect.ClassTag

object Jackson {
  class ScalaJsonMapper(jsonMapper: JsonMapper) extends JsonMapper(jsonMapper) with ClassTagExtensions

  val json: ScalaJsonMapper = {
    val builder = JsonMapper.builder()
    setMapperBuilderDefaults(builder)
    builder.enable(JsonParser.Feature.ALLOW_COMMENTS)
    new ScalaJsonMapper(builder.build())
  }

  private[stoys] def setMapperBuilderDefaults[M <: ObjectMapper, B <: MapperBuilder[M, B]](
      builder: MapperBuilder[M, B]): MapperBuilder[M, B] = {
    builder.findAndAddModules()
    builder.propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
    builder.enable(SerializationFeature.INDENT_OUTPUT)
    builder.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
    builder.defaultMergeable(true)
    builder.defaultPropertyInclusion(JsonInclude.Value.empty()
        .withContentInclusion(JsonInclude.Include.NON_ABSENT).withValueInclusion(JsonInclude.Include.NON_ABSENT))
//    builder.defaultSetterInfo(JsonSetter.Value.empty().withContentNulls(Nulls.AS_EMPTY).withValueNulls(Nulls.AS_EMPTY))
    builder
  }

//  private val emptyJsonNode = json.readTree("{}")
//
//  def empty[T: ClassTag]: T = {
//    json.treeToValue[T](emptyJsonNode)
//  }

  def deepCopy[T: ClassTag](originalValue: T): T = {
    json.treeToValue[T](json.valueToTree[JsonNode](originalValue))
  }
}

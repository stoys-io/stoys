package com.nuna.trustdb.core.util

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.fasterxml.jackson.databind.{ObjectMapper, PropertyNamingStrategy, SerializationFeature}
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Jackson {
  val objectMapper = createObjectMapper()
  val yamlObjectMapper = createObjectMapper(new YAMLFactory())
  val propertiesObjectMapper = createObjectMapper(new JavaPropsFactory())

  class ObjectMapperWithScalaMixin(jsonFactory: JsonFactory) extends ObjectMapper(jsonFactory) with ScalaObjectMapper

  def createObjectMapper(jsonFactory: JsonFactory = null): ObjectMapperWithScalaMixin = {
    val objectMapper = new ObjectMapperWithScalaMixin(jsonFactory)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.findAndRegisterModules()
    objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT)
    objectMapper.enable(JsonParser.Feature.ALLOW_COMMENTS)
    objectMapper.setDefaultPropertyInclusion(
      JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS))
    objectMapper
  }
}

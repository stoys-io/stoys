package com.nuna.trustdb.core.util

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.fasterxml.jackson.databind.{ObjectMapper, PropertyNamingStrategy, SerializationFeature}
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.datatype.jsr310.deser.{LocalDateDeserializer, LocalDateTimeDeserializer}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Jackson {
  val LOCALE_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd")
  val LOCALE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  val objectMapper = createObjectMapper()
  val yamlObjectMapper = createObjectMapper(new YAMLFactory())
  val propertiesObjectMapper = createObjectMapper(new JavaPropsFactory())

  class ObjectMapperWithScalaMixin(jsonFactory: JsonFactory) extends ObjectMapper(jsonFactory) with ScalaObjectMapper

  def createObjectMapper(jsonFactory: JsonFactory = null): ObjectMapperWithScalaMixin = {
    val objectMapper = new ObjectMapperWithScalaMixin(jsonFactory)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.findAndRegisterModules()

    val javaTimeModule = new JavaTimeModule()
        .addDeserializer(classOf[LocalDate], new LocalDateDeserializer(LOCALE_DATE_FORMATTER))
        .addDeserializer(classOf[LocalDateTime], new LocalDateTimeDeserializer(LOCALE_DATE_TIME_FORMATTER))
    objectMapper.registerModule(javaTimeModule)

    objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT)
    objectMapper.enable(JsonParser.Feature.ALLOW_COMMENTS)
    objectMapper.setDefaultPropertyInclusion(
      JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS))
    objectMapper
  }
}

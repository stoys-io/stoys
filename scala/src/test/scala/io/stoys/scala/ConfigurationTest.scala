package io.stoys.scala

import java.time.LocalDate

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonProperty.Access
import com.fasterxml.jackson.databind.JsonNode
import io.stoys.scala.Configuration.ConfigurationConfig
import org.scalatest.funsuite.AnyFunSuite

class ConfigurationTest extends AnyFunSuite {
  import ConfigurationTest._

  private val emptyTestConfig = Arbitrary.empty[TestConfig]
  private val defaultTestConfig = emptyTestConfig.copy(int = 42)
  private val fooTestConfig = defaultTestConfig.copy(string = "foo", seq = Seq("foo"), map = Map("foo" -> "foo"))

  test("parseConfigurationConfig") {
    assert(Configuration.parseConfigurationConfig(Array("environments=foo"))
        === ConfigurationConfig(Seq("local"), Seq("environments=foo")))
    assert(Configuration.parseConfigurationConfig(Array("--environments=foo"))
        === ConfigurationConfig(Seq("foo", "local"), Seq.empty))
    assert(Configuration.parseConfigurationConfig(Array("--environments=foo,bar", "--key=value", "some_arg"))
        === ConfigurationConfig(Seq("foo", "bar", "local"), Seq("--key=value", "some_arg")))
  }

  test("readConfig") {
    assert(Configuration().readConfig[TestConfig] === defaultTestConfig)
    assert(Configuration("--environments=foo").readConfig[TestConfig] === fooTestConfig)
    val envArgs = Configuration("--environments=foo", "non_existing__keys=are_ignored", "test_config__seq__0=prop",
      "test_config__map__some.key=value", "test_config__date=2020-02-02", "test_config__password=secret")
    assert(envArgs.readConfig[TestConfig]
        === TestConfig(42, "foo", Seq("prop"), Map("some.key" -> "value"), LocalDate.of(2020, 2, 2), "secret"))
    assert(Configuration("--environments=main").readConfig[TestConfig] === defaultTestConfig.copy(string = "main"))
    assert(Configuration("test_config__string=double_underscores__in___values__are_fine").readConfig[TestConfig]
        === defaultTestConfig.copy(string = "double_underscores__in___values__are_fine"))
  }

  test("allowInRootPackage") {
    assert(Configuration("--environments=foo").readConfig[TestConfig] === fooTestConfig)
    assert(Configuration("--environments=foo,root").readConfig[TestConfig] === fooTestConfig.copy(string = "root"))
  }

  test("prettyPrintJsonNode") {
    val node = Jackson.objectMapper.valueToTree[JsonNode](defaultTestConfig.copy(password = "secret"))
    val expected =
      """
        |{
        |  "int" : 42,
        |  "seq" : [ ],
        |  "map" : { }
        |}""".stripMargin.trim
    assert(Configuration.prettyPrintJsonNode[TestConfig](node) === expected)
  }

  test("updateCaseClassWithConfigMap") {
    assert(emptyTestConfig.string === null)
    assert(Configuration.updateCaseClassWithConfigMap(emptyTestConfig, null) === emptyTestConfig)
    assert(Configuration.updateCaseClassWithConfigMap(emptyTestConfig, Map.empty) === emptyTestConfig)
    val expected = emptyTestConfig.copy(int = 42, string = "foo")
    val anyConfigMap = Map("int" -> 42, "string" -> "foo")
    assert(Configuration.updateCaseClassWithConfigMap(emptyTestConfig, anyConfigMap) === expected)
    val stringConfigMap = Map("int" -> "42", "string" -> "foo")
    assert(Configuration.updateCaseClassWithConfigMap(emptyTestConfig, stringConfigMap) === expected)
    assert(emptyTestConfig.string === null)
  }
}

object ConfigurationTest {
  @Params(allowInRootPackage = true)
  case class TestConfig(
      int: Int,
      string: String,
      seq: Seq[String],
      map: Map[String, String],
      date: LocalDate,
      @JsonProperty(access = Access.WRITE_ONLY)
      password: String
  )
}

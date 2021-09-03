package io.stoys.scala

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonProperty.Access
import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

class ConfigurationTest extends AnyFunSuite {
  import Configuration._
  import ConfigurationTest._

  private lazy val emptyTestConfig = Arbitrary.empty[TestConfig]
  private lazy val defaultTestConfig = emptyTestConfig.copy(int = 42)
  private lazy val fooTestConfig = defaultTestConfig.copy(str = "foo", seq = Seq("foo"), map = Map("foo" -> "foo"))

  test("parseConfigurationConfig") {
    assert(parseConfigurationConfig(Array("environments=foo"))
        === ConfigurationConfig(Seq("local"), Seq("environments=foo")))
    assert(parseConfigurationConfig(Array("--environments=foo"))
        === ConfigurationConfig(Seq("foo", "local"), Seq.empty))
    assert(parseConfigurationConfig(Array("--environments=foo,bar", "--key=value", "some_arg"))
        === ConfigurationConfig(Seq("foo", "bar", "local"), Seq("--key=value", "some_arg")))
  }

  test("readConfig") {
    assert(Configuration().readConfig[TestConfig] === defaultTestConfig)
    assert(Configuration("--environments=foo").readConfig[TestConfig] === fooTestConfig)
    val envArgs = Configuration("--environments=foo", "non_existing__keys=are_ignored", "test_config__seq__0=prop",
      "test_config__map__some.key=value", "test_config__date=2020-02-02", "test_config__password=secret")
    assert(envArgs.readConfig[TestConfig] === TestConfig(42, "foo", Seq("foo", "prop"),
      Map("foo" -> "foo", "some.key" -> "value"), LocalDate.of(2020, 2, 2), "secret"))
    assert(Configuration("--environments=main").readConfig[TestConfig] === defaultTestConfig.copy(str = "main"))
    assert(Configuration("test_config__str=double_underscores__in___values__are_fine").readConfig[TestConfig]
        === defaultTestConfig.copy(str = "double_underscores__in___values__are_fine"))
  }

  test("allowInRootPackage") {
    assert(Configuration("--environments=foo").readConfig[TestConfig] === fooTestConfig)
    assert(Configuration("--environments=foo,root").readConfig[TestConfig] === fooTestConfig.copy(str = "root"))
  }

  test("prettyPrintJsonNode") {
    val node = Jackson.json.valueToTree[JsonNode](defaultTestConfig.copy(int = 1234, password = "secret"))
    val expected =
      """
        |{
        |  "int" : 1234,
        |  "seq" : [ ],
        |  "map" : { }
        |}""".stripMargin.trim
    assert(prettyPrintJsonNode[TestConfig](node) === expected)
  }
}

object ConfigurationTest {
  @Params(allowInRootPackage = true)
  case class TestConfig(
      int: Int,
      str: String,
      seq: Seq[String],
      map: Map[String, String],
      date: LocalDate,
      @JsonProperty(access = Access.WRITE_ONLY)
      password: String
  )
}

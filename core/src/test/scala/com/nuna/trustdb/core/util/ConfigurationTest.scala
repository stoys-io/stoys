package com.nuna.trustdb.core.util

import java.time.LocalDate

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonProperty.Access
import com.nuna.trustdb.core.util.Configuration.ConfigurationConfig
import org.scalatest.funsuite.AnyFunSuite

class ConfigurationTest extends AnyFunSuite {
  import ConfigurationTest._

  val emptyTestConfig = Arbitrary.empty[TestConfig]
  val defaultTestConfig = emptyTestConfig.copy(int = 42)
  val fooTestConfig = defaultTestConfig.copy(string = "foo", seq = Seq("foo"), map = Map("foo" -> "foo"))

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
    val envArgs = Configuration("--environments=foo", "unused@@string=unused", "test_config@@seq@@0=prop",
      "test_config@@map@@some.key=value", "test_config@@date=20191104", "test_config@@password=secret")
    assert(envArgs.readConfig[TestConfig]
        === TestConfig(42, "foo", Seq("prop"), Map("some.key" -> "value"), LocalDate.of(2019, 11, 4), "secret"))
    assert(Configuration("--environments=master").readConfig[TestConfig] === defaultTestConfig.copy(string = "master"))
  }

  test("allowInRootPackage") {
    assert(Configuration("--environments=foo").readConfig[TestConfig] === fooTestConfig)
    assert(Configuration("--environments=foo,root").readConfig[TestConfig] === fooTestConfig.copy(string = "root"))
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

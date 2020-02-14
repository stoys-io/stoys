package com.nuna.trustdb.core.util

import java.time.LocalDate

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonProperty.Access
import com.nuna.trustdb.core.util.Configuration.ConfigurationConfig
import org.scalatest.funsuite.AnyFunSuite

class ConfigurationTest extends AnyFunSuite {
  import ConfigurationTest._

  test("parseConfigurationConfig") {
    assert(Configuration.parseConfigurationConfig(Array("environments=foo"))
        === ConfigurationConfig(Seq("local"), Seq("environments=foo")))
    assert(Configuration.parseConfigurationConfig(Array("--environments=foo"))
        === ConfigurationConfig(Seq("foo", "local"), Seq.empty))
    assert(Configuration.parseConfigurationConfig(Array("--environments=foo,bar", "some_flag"))
        === ConfigurationConfig(Seq("foo", "bar", "local"), Seq("some_flag")))
  }

  test("readConfig") {
    val noArgs = new Configuration(Array.empty)
    assert(noArgs.readConfig[TestConfig] === TestConfig(42, null, Seq.empty, Map.empty, null, null))
    val env = new Configuration(Array("--environments=foo"))
    assert(env.readConfig[TestConfig] === TestConfig(42, "foo", Seq("foo"), Map("foo" -> "foo"), null, null))
    val envArgs = new Configuration(Array("--environments=foo", "unused@@string=unused", "test_config@@string=prop",
      "test_config@@map@@some.key=value", "test_config@@date=20191104", "test_config@@password=1234"))
    assert(envArgs.readConfig[TestConfig]
        === TestConfig(42, "prop", Seq("foo"), Map("some.key" -> "value"), LocalDate.of(2019, 11, 4), "1234"))
    val masterEnv = new Configuration(Array("--environments=master"))
    assert(masterEnv.readConfig[TestConfig] === TestConfig(42, "master", Seq.empty, Map.empty, null, null))
  }

  test("allowInRootPackage") {
    val fooEnv = new Configuration(Array("--environments=foo"))
    assert(fooEnv.readConfig[TestConfig] === TestConfig(42, "foo", Seq("foo"), Map("foo" -> "foo"), null, null))
    val fooRootEnv = new Configuration(Array("--environments=foo,root"))
    assert(fooRootEnv.readConfig[TestConfig] === TestConfig(42, "root", Seq("foo"), Map("foo" -> "foo"), null, null))
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

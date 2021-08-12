package io.stoys.scala

import org.scalatest.funsuite.AnyFunSuite

class StringsTest extends AnyFunSuite {
  import Strings._
  import StringsTest._

  test("toCamelCase") {
    assert(toCamelCase("Foo") === "Foo")
    assert(toCamelCase("FooBarBaz") === "FooBarBaz")
    assert(toCamelCase("foo_bar") === "FooBar")
    assert(toCamelCase("foo__bar") === "Foo_Bar")
    assert(toCamelCase("Foo_Bar") === "FooBar")
    assert(toCamelCase("Foo42") === "Foo42")
    assert(toCamelCase("Foo42Bar") === "Foo42Bar")
    assert(toCamelCase("FooBARBaz") === "FooBARBaz")
    assert(toCamelCase("FOOBARBaz") === "FOOBARBaz")
    assert(toCamelCase("foo  bar_ Baz") === "Foo_Bar_Baz")
    assert(toCamelCase("Foo_bAR_Baz") === "FooBARBaz")
    assert(toCamelCase("foo  Bar__bAZ") === "Foo_Bar_BAZ")
  }

  test("toSnakeCase") {
    assert(toSnakeCase("Foo") === "foo")
    assert(toSnakeCase("FooBarBaz") === "foo_bar_baz")
    assert(toSnakeCase("foo_bar") === "foo_bar")
    assert(toSnakeCase("foo__bar") === "foo__bar")
    assert(toSnakeCase("Foo_Bar") === "foo_bar")
    assert(toSnakeCase("Foo42") === "foo42")
    assert(toSnakeCase("Foo42Bar") === "foo42_bar")
    assert(toSnakeCase("FooBARBaz") === "foo_bar_baz")
    assert(toSnakeCase("FOOBARBaz") === "foobar_baz")
    assert(toSnakeCase("foo  bar_ Baz") === "foo__bar__baz")
    assert(toSnakeCase("Foo_BAR_Baz") === "foo_bar_baz")
    assert(toSnakeCase("foo  Bar__bAZ") === "foo__bar__b_az")
  }

  test("replaceParams by case class") {
    val params = Params(value = "bar")
    assert(replaceParams("foo ${value} baz", Some(params)) === "foo bar baz")
    assertThrows[NoSuchFieldException](replaceParams("foo ${non_existing_param} baz", Some(params)))
    assert(replaceParams("foo ${whatever} baz", None) === "foo ${whatever} baz")
  }

  test("replaceParams by Map") {
    val params = Map("value" -> "bar")
    assert(replaceParams("foo ${value} baz", params) === "foo bar baz")
    assertThrows[NoSuchElementException](replaceParams("foo ${non_existing_param} baz", params))
  }

  test("trim") {
    assert(trim(null) === None)
    assert(trim("") === None)
    assert(trim(" ") === None)
    assert(trim("\n \t") === None)
    assert(trim(" foo  bar \n  \t ") === Some("foo  bar"))
  }

  test("toSqlStringLiterals") {
    assert(toSqlListLiterals(Seq("foo", "bar", "baz")) === """("foo","bar","baz")""")
    assert(toSqlListLiterals(Seq("foo")) === """("foo")""")
    assert(toSqlListLiterals(Seq[String]()) == """()""")
  }

  test("toWordCharacters") {
    assert(toWordCharacters("foo") === "foo")
    assert(toWordCharacters("foo -:? bar") === "foo_____bar")
    assert(toWordCharacters("foo =^_^= bar") === "foo_______bar")
  }

  test("toWordCharactersCollapsing") {
    assert(toWordCharactersCollapsing("foo") === "foo")
    assert(toWordCharactersCollapsing("foo -:? bar") === "foo_bar")
    assert(toWordCharactersCollapsing("foo =^_^= bar") === "foo_bar")
  }

  test("unsafeRemoveLineComments") {
    assert(unsafeRemoveLineComments("foo\nbar", "#") === "foo\nbar")
    assert(unsafeRemoveLineComments("foo\n#bar", "#") === "foo\n")
    assert(unsafeRemoveLineComments("foo\nbar # meh\nbaz", "#") === "foo\nbar \nbaz")
    assert(unsafeRemoveLineComments("foo\n--bar\nbaz", "--") === "foo\n\nbaz")
  }
}

object StringsTest {
  case class Params(value: String)
}

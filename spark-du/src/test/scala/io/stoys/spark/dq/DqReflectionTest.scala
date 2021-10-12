package io.stoys.spark.dq

import io.stoys.spark.SToysException
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Date

class DqReflectionTest extends AnyFunSuite {
  import DqReflection._
  import DqReflectionTest._
  import DqRules._

  test("getDqFields - Record") {
    val expectedFields = Seq(
      field("i", "\"integer\""),
      field("d", "\"date\"", format = "MM/dd/yyyy"),
      field("e", "\"string\"", enumValues = Seq("foo", "bar", "baz")),
    )
    assert(getDqFields[Record] === expectedFields)
  }

  test("getDqFields - NullableRecord") {
    val expectedFields = Seq(
      field("i", "\"integer\""),
      field("s", "\"string\""),
      field("s_option", "\"string\""),
      field("i__nullable", "\"integer\""),
      field("i__not_nullable", "\"integer\"", nullable = false),
      field("s__not_nullable", "\"string\"", nullable = false),
      field("s_option__not_nullable", "\"string\"", nullable = false),
    )
    assert(getDqFields[NullableRecord] === expectedFields)
  }

  test("getDqFields - CollectionRecord") {
    val expectedFields = Seq(
      field("i_seq", """{"type":"array","elementType":"integer","containsNull":false}"""),
      field("s_map", """{"type":"map","keyType":"string","valueType":"string","valueContainsNull":true}"""),
    )
    assert(getDqFields[CollectionRecord] === expectedFields)
  }

  test("getDqFields - PoorlyNamedRecord") {
    val expectedFields = Seq(
      field("s +", "\"string\""),
      field("s -", "\"string\""),
      field("mixed_CASE", "\"string\""),
    )
    assert(getDqFields[PoorlyNamedRecord] === expectedFields)
  }

  test("getDqFields - UnsupportedRecord") {
    assertThrows[SToysException](getDqFields[UnsupportedRecord])
    assert(getDqFields[UnsupportedRecord](ignoreUnsupportedTypes = true) === Seq.empty)
  }
}

object DqReflectionTest {
  import annotation.DqField

  case class Record(
      i: Int,
      @DqField(format = "MM/dd/yyyy")
      d: Date,
      @DqField(enumValues = Array("foo", "bar", "baz"))
      e: String,
      @DqField(ignore = true)
      ignored: String,
  )

  case class NullableRecord(
      i: Int,
      s: String,
      s_option: Option[String],
      @DqField(nullable = true)
      i__nullable: Int,
      @DqField(nullable = false)
      i__not_nullable: Int,
      @DqField(nullable = false)
      s__not_nullable: String,
      @DqField(nullable = false)
      s_option__not_nullable: Option[String],
  )

  case class CollectionRecord(
      i_seq: Seq[Int],
      s_map: Map[String, String],
  )

  case class PoorlyNamedRecord(
      `s +`: String,
      `s -`: String,
      mixed_CASE: String,
  )

  case class UnsupportedRecord(
      unsupported: java.util.UUID,
  )
}

package io.stoys.spark.dq

import io.stoys.scala.Arbitrary
import io.stoys.spark.SToysException
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Date

class DqReflectionTest extends AnyFunSuite {
  import DqReflection._
  import DqReflectionTest._

  test("basics") {
    val expectedFields = Seq(
      DqField("id", "\"integer\"", nullable = true, Seq.empty, None, None),
      DqField("custom_date", "\"date\"", nullable = true, Seq.empty, Some("MM/dd/yyyy"), None),
      DqField("custom_enum", "\"string\"", nullable = false, Seq("foo", "bar", "baz"), None, None)
    )
    assert(getDqFields[Record] === expectedFields)
  }

  test("nullability") {
    val emptyDqField = Arbitrary.empty[DqField]
    val expectedFields = Seq(
      emptyDqField.copy(name = "int", data_type_json = "\"integer\"", nullable = true),
      emptyDqField.copy(name = "str", data_type_json = "\"string\"", nullable = true),
      emptyDqField.copy(name = "option_str", data_type_json = "\"string\"", nullable = true),
      emptyDqField.copy(name = "dq_field_not_nullable_int", data_type_json = "\"integer\"", nullable = false),
      emptyDqField.copy(name = "dq_field_not_nullable_str", data_type_json = "\"string\"", nullable = false),
      emptyDqField.copy(name = "dq_field_not_nullable_option_str", data_type_json = "\"string\"", nullable = false)
    )
    assert(getDqFields[NullableRecord] === expectedFields)
  }

  test("collections") {
    val emptyDqField = Arbitrary.empty[DqField]
    val intSeqDataTypeJson = """{"type":"array","elementType":"integer","containsNull":false}"""
    val strMapDataTypeJson = """{"type":"map","keyType":"string","valueType":"string","valueContainsNull":true}"""
    val expectedFields = Seq(
      emptyDqField.copy(name = "int_seq", data_type_json = intSeqDataTypeJson, nullable = true),
      emptyDqField.copy(name = "str_map", data_type_json = strMapDataTypeJson, nullable = true)
    )
    assert(getDqFields[CollectionRecord] === expectedFields)
  }

  test("poorly named") {
    val emptyDqField = Arbitrary.empty[DqField]
    val expectedFields = Seq(
      emptyDqField.copy(name = "s +", data_type_json = "\"string\"", nullable = true),
      emptyDqField.copy(name = "s -", data_type_json = "\"string\"", nullable = true),
      emptyDqField.copy(name = "mixed_CASE", data_type_json = "\"string\"", nullable = true),
    )
    assert(getDqFields[PoorlyNamedRecord] === expectedFields)
  }

  test("unsupported") {
    assertThrows[SToysException](getDqFields[UnsupportedRecord])
    assert(getDqFields[UnsupportedRecord](ignoreUnsupportedTypes = true) === Seq.empty)
  }
}

object DqReflectionTest {
  import annotation.DqField

  case class Record(
      id: Int,
      @DqField(nullable = true, format = "MM/dd/yyyy")
      custom_date: Date,
      @DqField(nullable = false, enumValues = Array("foo", "bar", "baz"))
      custom_enum: String,
      @DqField(ignore = true)
      ignored: String
  )

  case class NullableRecord(
      int: Int,
      str: String,
      option_str: Option[String],
      @DqField(nullable = false)
      dq_field_not_nullable_int: Int,
      @DqField(nullable = false)
      dq_field_not_nullable_str: String,
      @DqField(nullable = false)
      dq_field_not_nullable_option_str: Option[String]
  )

  case class CollectionRecord(
      int_seq: Seq[Int],
      str_map: Map[String, String]
  )

  case class PoorlyNamedRecord(
      `s +`: String,
      `s -`: String,
      mixed_CASE: String
  )

  case class UnsupportedRecord(
      unsupported: java.util.UUID
  )
}

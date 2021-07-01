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
      DqField("id", "integer", nullable = true, Seq.empty, None, None),
      DqField("custom_date", "date", nullable = true, Seq.empty, Some("MM/dd/yyyy"), None),
      DqField("custom_enum", "string", nullable = false, Seq("foo", "bar", "baz"), None, None)
    )
    assert(getDqFields[Record] === expectedFields)
  }

  test("nullability") {
    val emptyDqField = Arbitrary.empty[DqField]
    val expectedFields = Seq(
      emptyDqField.copy(name = "int", typ = "integer", nullable = true),
      emptyDqField.copy(name = "str", typ = "string", nullable = true),
      emptyDqField.copy(name = "option_str", typ = "string", nullable = true),
      emptyDqField.copy(name = "dq_field_not_nullable_int", typ = "integer", nullable = false),
      emptyDqField.copy(name = "dq_field_not_nullable_str", typ = "string", nullable = false),
      emptyDqField.copy(name = "dq_field_not_nullable_option_str", typ = "string", nullable = false)
    )
    assert(getDqFields[NullableRecord] === expectedFields)
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
      customDate: Date,
      @DqField(nullable = false, enumValues = Array("foo", "bar", "baz"))
      customEnum: String,
      @DqField(ignore = true)
      ignored: String
  )

  case class NullableRecord(
      int: Int,
      str: String,
      optionStr: Option[String],
      @DqField(nullable = false)
      dqFieldNotNullableInt: Int,
      @DqField(nullable = false)
      dqFieldNotNullableStr: String,
      @DqField(nullable = false)
      dqFieldNotNullableOptionStr: Option[String]
  )

  case class UnsupportedRecord(
      unsupported: java.util.UUID
  )
}

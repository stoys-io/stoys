package io.stoys.spark.dq

import java.sql.Date

import org.scalatest.funsuite.AnyFunSuite

class DqReflectionTest extends AnyFunSuite {
  import DqReflection._
  import DqReflectionTest._

  test("getDqFields") {
    val expectedFields = Seq(
      DqField("id", "integer", nullable = false, Seq.empty, None, None),
      DqField("custom_date", "date", nullable = true, Seq.empty, Some("MM/dd/yyyy"), None),
      DqField("custom_enum", "string", nullable = false, Seq("foo", "bar", "baz"), None, None)
    )
    assert(getDqFields[Record] === expectedFields)
  }
}

object DqReflectionTest {
  import annotation.DqField

  case class Record(
      id: Int,
      @DqField(nullable = true, format = "MM/dd/yyyy")
      customDate: Date,
      @DqField(nullable = false, enumValues = Array("foo", "bar", "baz"))
      customEnum: String
  )
}

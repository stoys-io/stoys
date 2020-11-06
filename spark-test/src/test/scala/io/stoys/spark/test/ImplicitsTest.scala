package io.stoys.spark.test

class ImplicitsTest extends SparkTestBase {
  test("implicits") {
    assertTypeError("val stringOption: Option[String] = \"2020-02-20\"")
    assertTypeError("val sqlDate: java.sql.Date = \"2020-02-20\"")
    assertTypeError("val sqlDate: java.util.Date = \"2020-02-20\"")
    assertTypeError("val sqlDate: java.time.LocalDate = \"2020-02-20\"")

    import io.stoys.spark.test.implicits._
    assert(("2020-02-20": Option[String]) === Some("2020-02-20"))
    assert(("2020-02-20": java.sql.Date) === java.sql.Date.valueOf("2020-02-20"))
    assert(("2020-02-20": java.util.Date) === java.sql.Date.valueOf("2020-02-20"))
    assert(("2020-02-20": java.time.LocalDate) === java.time.LocalDate.parse("2020-02-20"))
  }
}

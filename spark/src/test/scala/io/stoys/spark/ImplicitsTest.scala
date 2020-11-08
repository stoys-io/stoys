package io.stoys.spark

import io.stoys.spark.test.SparkTestBase

class ImplicitsTest extends SparkTestBase {
  import ImplicitsTest._

  test("implicits") {
    val df = sparkSession.sql("SELECT '42' AS str, 42 AS int")
    assertTypeError("df.reshape[Record]")
    import io.stoys.spark.implicits._
    assert(df.reshape[Record].columns === Seq("str"))
  }
}

object ImplicitsTest {
  case class Record(str: String)
}

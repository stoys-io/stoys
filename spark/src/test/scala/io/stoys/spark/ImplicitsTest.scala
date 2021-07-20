package io.stoys.spark

import io.stoys.spark.test.SparkTestBase

class ImplicitsTest extends SparkTestBase {
  import ImplicitsTest._

  test("implicits") {
    val df = sparkSession.sql("SELECT '42' AS s, 42 AS i")

    assertTypeError("df.reshape[Record]")

    import io.stoys.spark.implicits._
    assert(df.reshape[Record].columns === Seq("s"))
  }
}

object ImplicitsTest {
  case class Record(s: String)
}

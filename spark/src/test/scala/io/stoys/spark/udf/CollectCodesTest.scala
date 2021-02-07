package io.stoys.spark.udf

import io.stoys.spark.test.SparkTestBase
import org.apache.spark.SparkException

class CollectCodesTest extends SparkTestBase {
  import CollectCodesTest._
  import sparkSession.implicits._

  sparkSession.udf.register("collect_codes", new CollectCodes())
  sparkSession.udf.register("cleanup_codes", CollectCodes.cleanupCodes _)

  def selectSingleStringSeq(query: String): Seq[String] = {
    sparkSession.sql(query).collect().head.getSeq[String](0)
  }

  def mapped(expression: String): Seq[String] = {
    selectSingleStringSeq(s"SELECT $expression AS codes FROM foo")
  }

  def grouped(expression: String): Seq[String] = {
    selectSingleStringSeq(s"SELECT $expression AS codes FROM foo GROUP BY true")
  }

  def joined(expression: String): Seq[String] = {
    selectSingleStringSeq(s"SELECT $expression AS codes FROM foo JOIN bar ON foo.id = bar.id")
  }

  test("CollectCodes") {
    val foo = Seq(Record(1, Seq("c", "", "a"), Seq("foo1ac")), Record(2, Seq("", null, "b"), Seq("foo2ac")))
    foo.toDS().createOrReplaceTempView("foo")

    val bar = Seq(Record(2, Seq("bar", "", null), Seq("bar2ac")))
    bar.toDS().createOrReplaceTempView("bar")

    assert(mapped("COLLECT_CODES(codes, alternative_codes)") === Seq("a", "b", "c", "foo1ac", "foo2ac"))

    assert(grouped("ARRAY_SORT(FLATTEN(COLLECT_LIST(codes)))") === Seq("", "", "a", "b", "c", null))
    assert(grouped("COLLECT_CODES(codes)") === Seq("a", "b", "c"))
    val groupFix = "ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(codes)))"
    assert(grouped(s"ARRAY_SORT(FILTER($groupFix, c -> LENGTH(c) > 0))") === Seq("a", "b", "c"))

    assert(joined("COLLECT_CODES(foo.codes, bar.codes, 'meh', 'b', '', null)") === Seq("b", "bar", "meh"))
    val joinFix = "ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(ARRAY_UNION(ARRAY_UNION(foo.codes, bar.codes), ARRAY('meh')))))"
    assert(joined(s"ARRAY_SORT(FILTER($joinFix, c -> LENGTH(c) > 0))") === Seq("b", "bar", "meh"))

    val foobar = Seq("bar", "foo")
    assert(selectSingleStringSeq("SELECT COLLECT_CODES('foo', 'bar')") === foobar)
    assert(selectSingleStringSeq("SELECT COLLECT_CODES(code) FROM VALUES ('foo'), ('bar') AS (code)") === foobar)
    assert(selectSingleStringSeq(
      "SELECT COLLECT_CODES(codes1, codes2) FROM VALUES (array('foo'), array('bar')) AS (codes1, codes2)") === foobar)

    assertThrows[SparkException](selectSingleStringSeq("SELECT COLLECT_CODES(42)"))
  }
}

object CollectCodesTest {
  case class Record(id: Long, codes: Seq[String], alternative_codes: Seq[String] = Seq.empty[String])
}

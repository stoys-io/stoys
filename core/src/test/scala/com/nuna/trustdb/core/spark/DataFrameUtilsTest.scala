package com.nuna.trustdb.core.spark

import com.nuna.trustdb.core.SparkTestBase

class DataFrameUtilsTest extends SparkTestBase {
  import DataFrameUtilsTest._
  import sparkSession.implicits._

  lazy val records = Seq(Record("foo", "bar", NestedRecord("baz"))).toDF()

  test("projection") {
    val subsetOfRecords = DataFrameUtils.asDataset[SubsetOfRecord](records)
    assert(subsetOfRecords.columns === Seq("foo", "nested"))
  }

  test("reordering") {
    val recordsWithReorderedColumns = records.select("bar", "foo", "nested")
    assert(recordsWithReorderedColumns.columns === Seq("bar", "foo", "nested"))
    val recordsWithProperlyReorderedColumns = DataFrameUtils.asDataset[Record](recordsWithReorderedColumns)
    assert(recordsWithProperlyReorderedColumns.columns === Seq("foo", "bar", "nested"))
  }

  test("missing columns") {
    val caught = intercept[RuntimeException](DataFrameUtils.asDataset[MissingColumnRecord](records))
    assert(caught.getMessage.contains("missing columns"))
    assert(caught.getMessage.contains("missing"))
  }

  test("duplicate columns") {
    val recordsWithDuplicateColumns = records.select("foo", "foo", "nested")
    val caught = intercept[RuntimeException](DataFrameUtils.asDataset[SubsetOfRecord](recordsWithDuplicateColumns))
    assert(caught.getMessage.contains("duplicate columns"))
    assert(caught.getMessage.contains("foo"))
  }

  test("implicits") {
    import com.nuna.trustdb.core.spark.implicits._
    val recordsWithReorderedColumns = records.select("bar", "foo", "nested")
    val actual = recordsWithReorderedColumns.asDataset[SubsetOfRecord]
    assert(actual.columns === Seq("foo", "nested"))
  }
}

object DataFrameUtilsTest {
  case class NestedRecord(baz: String)
  case class Record(foo: String, bar: String, nested: NestedRecord)
  case class SubsetOfRecord(foo: String, nested: NestedRecord)
  case class MissingColumnRecord(missing: String)
}

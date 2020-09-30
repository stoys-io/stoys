package com.nuna.trustdb.core.spark

import com.nuna.trustdb.core.SparkTestBase

class DatasetsTest extends SparkTestBase {
  import DatasetsTest._
  import sparkSession.implicits._

  lazy val records = Seq(Record("foo", 42, NestedRecord("nested"))).toDF()

  test("projection") {
    val subsetOfRecords = Datasets.asDataset[SubsetOfRecord](records)
    assert(subsetOfRecords.columns === Seq("str", "nested"))
  }

  test("reordering") {
    val recordsWithReorderedColumns = records.select("num", "str", "nested")
    assert(recordsWithReorderedColumns.columns === Seq("num", "str", "nested"))
    val recordsWithProperlyReorderedColumns = Datasets.asDataset[Record](recordsWithReorderedColumns)
    assert(recordsWithProperlyReorderedColumns.columns === Seq("str", "num", "nested"))
  }

  test("missing columns") {
    val caught = intercept[RuntimeException](Datasets.asDataset[NestedRecord](records))
    assert(caught.getMessage.contains("missing columns"))
    assert(caught.getMessage.contains("nestedStr"))
  }

  test("duplicate columns") {
    val recordsWithDuplicateColumns = records.select("str", "str", "nested")
    val caught = intercept[RuntimeException](Datasets.asDataset[SubsetOfRecord](recordsWithDuplicateColumns))
    assert(caught.getMessage.contains("duplicate columns"))
    assert(caught.getMessage.contains("str"))
  }

  test("implicits") {
    import com.nuna.trustdb.core.spark.implicits._
    val recordsWithReorderedColumns = records.select("num", "str", "nested")
    assert(recordsWithReorderedColumns.asDataset[SubsetOfRecord].columns === Seq("str", "nested"))
    val recordsWithReorderedColumnsDS = recordsWithReorderedColumns.as[SubsetOfRecord]
    assert(recordsWithReorderedColumnsDS.asDataset[SubsetOfRecord].columns === Seq("str", "nested"))
  }
}

object DatasetsTest {
  case class NestedRecord(nestedStr: String)
  case class Record(str: String, num: Int, nested: NestedRecord)
  case class SubsetOfRecord(str: String, nested: NestedRecord)
}

package com.nuna.trustdb.core.spark

import com.nuna.trustdb.core.SparkTestBase
import com.nuna.trustdb.core.spark.Datasets.StructValidationException

class DatasetsTest extends SparkTestBase {
  import DatasetsTest._
  import sparkSession.implicits._

  val records = Seq(Record("foo", 42, NestedRecord("nested")))
  lazy val recordsDF = records.toDF()

  test("asDataset - projection") {
    val subsetOfRecords = Datasets.asDataset[SubsetOfRecord](recordsDF)
    assert(subsetOfRecords.columns === Seq("str", "nested"))
  }

  test("asDataset - reordering") {
    val recordsWithReorderedColumns = recordsDF.select("num", "str", "nested")
    assert(recordsWithReorderedColumns.columns === Seq("num", "str", "nested"))
    val recordsWithProperlyReorderedColumns = Datasets.asDataset[Record](recordsWithReorderedColumns)
    assert(recordsWithProperlyReorderedColumns.columns === Seq("str", "num", "nested"))
  }

  test("asDataset - missing columns") {
    val caught = intercept[RuntimeException](Datasets.asDataset[NestedRecord](recordsDF))
    assert(caught.getMessage.contains("missing columns"))
    assert(caught.getMessage.contains("nestedStr"))
  }

  test("asDataset - duplicate columns") {
    val recordsWithDuplicateColumns = recordsDF.select("str", "str", "nested")
    val caught = intercept[RuntimeException](Datasets.asDataset[SubsetOfRecord](recordsWithDuplicateColumns))
    assert(caught.getMessage.contains("duplicate columns"))
    assert(caught.getMessage.contains("str"))
  }

  test("asDataset - implicits") {
    import com.nuna.trustdb.core.spark.implicits._
    val recordsWithReorderedColumns = recordsDF.select("num", "str", "nested")
    assert(recordsWithReorderedColumns.asDataset[SubsetOfRecord].columns === Seq("str", "nested"))
    val recordsWithReorderedColumnsDS = recordsWithReorderedColumns.as[SubsetOfRecord]
    assert(recordsWithReorderedColumnsDS.asDataset[SubsetOfRecord].columns === Seq("str", "nested"))
  }

  test("asDS - coerceTypes") {
    val fixableDF = recordsDF.selectExpr("42 AS str", "CAST(num AS byte) AS num", "nested")
    val fixedDS = Datasets.asDS[Record](fixableDF)
    assert(fixedDS.collect() === Seq(records.head.copy(str = "42")))
    val config = Datasets.Config.default.copy(coerceTypes = false)
    val caught = intercept[StructValidationException](Datasets.asDS[Record](fixableDF, config))
    assert(caught.getMessage.contains("str type IntegerType cannot be casted to target type StringType"))
    assert(caught.getMessage.contains("num type ByteType cannot be casted to target type IntegerType"))
  }

  ignore("asDS - conflictResolution") {
    ??? // TODO: implement conflict resolution
  }

  test("asDS - failOnDroppingExtraColumn") {
    val fixableDF = recordsDF.selectExpr("*", "'dropped_value' AS dropped_column")
    val fixedDS = Datasets.asDS[Record](fixableDF)
    assert(fixedDS.collect() === records)
    val config = Datasets.Config.default.copy(failOnDroppingExtraColumn = true)
    val caught = intercept[StructValidationException](Datasets.asDS[Record](fixableDF, config))
    assert(caught.getMessage.contains("dropped_column has been dropped"))
  }

  test("asDS - fillDefaultValues") {
    val fixableDF = sparkSession.sql("SELECT 'unused' AS dummy_column")
    val caught = intercept[StructValidationException](Datasets.asDS[Record](fixableDF))
    assert(caught.getMessage.contains("str is missing"))
    assert(caught.getMessage.contains("num is missing"))
    assert(caught.getMessage.contains("nested is missing"))
    val config = Datasets.Config.default.copy(fillDefaultValues = true)
    val fixedDS = Datasets.asDS[Record](fixableDF, config)
    assert(fixedDS.collect() === Array(Record("", 0, NestedRecord(""))))
  }

  test("asDS - fillMissingNulls") {
    val fixableDF = sparkSession.sql("SELECT 42 AS num")
    val caught = intercept[StructValidationException](Datasets.asDS[Record](fixableDF))
    assert(caught.getMessage.contains("str is missing"))
    assert(caught.getMessage.contains("nested is missing"))
    val config = Datasets.Config.default.copy(fillMissingNulls = true)
    val fixedDS = Datasets.asDS[Record](fixableDF, config)
    assert(fixedDS.collect() === Array(Record(null, 42, null)))
  }

  test("asDS - normalizedNameMatching") {
    val fixableDF = sparkSession.sql("SELECT 'foo' AS `nested str`")
    val caught = intercept[StructValidationException](Datasets.asDS[NestedRecord](fixableDF))
    assert(caught.getMessage.contains("nestedStr is missing"))
    val config = Datasets.Config.default.copy(normalizedNameMatching = true)
    val fixedDS = Datasets.asDS[NestedRecord](fixableDF, config)
    assert(fixedDS.collect() === Array(NestedRecord("foo")))
  }

  test("asDS - sortOrder") {
    val fixableDF = recordsDF.selectExpr("num", "nested", "str")
    assert(Datasets.asDS[Record](fixableDF).columns === Seq("str", "num", "nested"))
    val sourceOrderConfig = Datasets.Config.default.copy(sortOrder = Datasets.Config.SortOrder.SOURCE)
    assert(Datasets.asDS[Record](fixableDF, sourceOrderConfig).columns === Seq("num", "nested", "str"))
    val alphabeticalOrderConfig = Datasets.Config.default.copy(sortOrder = Datasets.Config.SortOrder.ALPHABETICAL)
    assert(Datasets.asDS[Record](fixableDF, alphabeticalOrderConfig).columns === Seq("nested", "num", "str"))
  }

  test("asDS - removeExtraColumns") {
    val df = recordsDF.selectExpr("*", "'foo' AS extra_column")
    val dsWithoutExtraColumns = Datasets.asDS[Record](df)
    assert(dsWithoutExtraColumns.columns === Seq("str", "num", "nested"))
    val config = Datasets.Config.default.copy(removeExtraColumns = false)
    val dsWithExtraColumns = Datasets.asDS[Record](df, config)
    assert(dsWithExtraColumns.columns === Seq("str", "num", "nested", "extra_column"))
  }

  test("asDS - arrays") {
    val df = sparkSession.sql("SELECT array(struct('str', 'foo')) AS records")
    val config = Datasets.Config.dangerous.copy(sortOrder = Datasets.Config.SortOrder.ALPHABETICAL)
    val ds = Datasets.asDS[SeqOfRecord](df, config)
    assert(ds.collect() === Seq(SeqOfRecord(Seq(Record(null, 0, null)))))
  }

  test("asDS - implicits") {
    import com.nuna.trustdb.core.spark.implicits._
    val df = recordsDF.selectExpr("str", "struct('foo', 'nested str') AS nested", "'foo' AS extra_column")
    val config = Datasets.Config.dangerous.copy(sortOrder = Datasets.Config.SortOrder.ALPHABETICAL)
    val ds = df.asDS[Record](config)
    assert(ds.columns === Seq("nested", "num", "str"))
  }
}

object DatasetsTest {
  case class NestedRecord(nestedStr: String)
  case class Record(str: String, num: Int, nested: NestedRecord)
  case class SubsetOfRecord(str: String, nested: NestedRecord)
  case class SeqOfRecord(records: Seq[Record])
}

package io.stoys.core.spark

import io.stoys.core.SparkTestBase
import io.stoys.core.spark.Datasets.{ReshapeConfig, StructValidationException}
import org.apache.spark.sql.types.{StructField, StructType}

class DatasetsTest extends SparkTestBase {
  import DatasetsTest._
  import sparkSession.implicits._

  val records = Seq(Record("foo", 42, NestedRecord("nested")))
  lazy val recordsDF = records.toDF()

  test("reshape - coerceTypes") {
    val fixableDF = recordsDF.selectExpr("42 AS str", "CAST(num AS byte) AS num", "nested")
    val fixedDS = Datasets.reshape[Record](fixableDF)
    assert(fixedDS.collect() === Seq(records.head.copy(str = "42")))
    val config = ReshapeConfig.default.copy(coerceTypes = false)
    val caught = intercept[StructValidationException](Datasets.reshape[Record](fixableDF, config))
    assert(caught.getMessage.contains("str type IntegerType cannot be casted to target type StringType"))
    assert(caught.getMessage.contains("num type ByteType cannot be casted to target type IntegerType"))
  }

  ignore("reshape - conflictResolution") {
    ??? // TODO: implement conflict resolution
  }

  test("reshape - dropExtraColumns") {
    val df = recordsDF.selectExpr("*", "'foo' AS extra_column")
    val dsWithoutExtraColumns = Datasets.reshape[Record](df)
    assert(dsWithoutExtraColumns.columns === Seq("str", "num", "nested"))
    val config = ReshapeConfig.default.copy(dropExtraColumns = false)
    val dsWithExtraColumns = Datasets.reshape[Record](df, config)
    assert(dsWithExtraColumns.columns === Seq("str", "num", "nested", "extra_column"))
  }

  test("reshape - failOnExtraColumn") {
    val fixableDF = recordsDF.selectExpr("*", "'foo' AS extra_column")
    val fixedDS = Datasets.reshape[Record](fixableDF)
    assert(fixedDS.collect() === records)
    val config = ReshapeConfig.default.copy(failOnExtraColumn = true)
    val caught = intercept[StructValidationException](Datasets.reshape[Record](fixableDF, config))
    assert(caught.getMessage.contains("extra_column unexpectedly present"))
  }

  test("reshape - failOnIgnoringNullability") {
    val nullableSchema = StructType(recordsDF.schema.fields.map(f => StructField(f.name, f.dataType, nullable = true)))
    val fixableDF = sparkSession.createDataFrame(recordsDF.rdd, nullableSchema)
    val fixedDS = Datasets.reshape[Record](fixableDF)
    assert(fixedDS.collect() === records)
    val config = ReshapeConfig.default.copy(failOnIgnoringNullability = true)
    val caught = intercept[StructValidationException](Datasets.reshape[Record](fixableDF, config))
    assert(caught.getMessage.contains("num is nullable but target column is not"))
  }

  test("reshape - fillDefaultValues") {
    val fixableDF = sparkSession.sql("SELECT 'unused' AS dummy_column")
    val caught = intercept[StructValidationException](Datasets.reshape[Record](fixableDF))
    assert(caught.getMessage.contains("str is missing"))
    assert(caught.getMessage.contains("num is missing"))
    assert(caught.getMessage.contains("nested is missing"))
    val config = ReshapeConfig.default.copy(fillDefaultValues = true)
    val fixedDS = Datasets.reshape[Record](fixableDF, config)
    assert(fixedDS.collect() === Seq(Record("", 0, NestedRecord(""))))
  }

  test("reshape - fillMissingNulls") {
    val fixableDF = sparkSession.sql("SELECT 42 AS num")
    val caught = intercept[StructValidationException](Datasets.reshape[Record](fixableDF))
    assert(caught.getMessage.contains("str is missing"))
    assert(caught.getMessage.contains("nested is missing"))
    val config = ReshapeConfig.default.copy(fillMissingNulls = true)
    val fixedDS = Datasets.reshape[Record](fixableDF, config)
    assert(fixedDS.collect() === Seq(Record(null, 42, null)))
  }

  test("reshape - normalizedNameMatching") {
    val fixableDF = sparkSession.sql("SELECT 'foo' AS `nested str`")
    val caught = intercept[StructValidationException](Datasets.reshape[NestedRecord](fixableDF))
    assert(caught.getMessage.contains("nestedstr is missing"))
    val config = ReshapeConfig.default.copy(normalizedNameMatching = true)
    val fixedDS = Datasets.reshape[NestedRecord](fixableDF, config)
    assert(fixedDS.collect() === Seq(NestedRecord("foo")))
  }

  test("reshape - sortOrder") {
    val fixableDF = recordsDF.selectExpr("num", "nested", "str")
    assert(Datasets.reshape[Record](fixableDF).columns === Seq("str", "num", "nested"))
    val sourceOrderConfig = ReshapeConfig.default.copy(sortOrder = ReshapeConfig.SortOrder.SOURCE)
    assert(Datasets.reshape[Record](fixableDF, sourceOrderConfig).columns === Seq("num", "nested", "str"))
    val alphabeticalOrderConfig = ReshapeConfig.default.copy(sortOrder = ReshapeConfig.SortOrder.ALPHABETICAL)
    assert(Datasets.reshape[Record](fixableDF, alphabeticalOrderConfig).columns === Seq("nested", "num", "str"))
  }

  test("reshape - arrays") {
    val df = sparkSession.sql("SELECT array(struct('str', 'foo')) AS records")
    val config = ReshapeConfig.dangerous.copy(sortOrder = ReshapeConfig.SortOrder.ALPHABETICAL)
    val ds = Datasets.reshape[SeqOfRecord](df, config)
    assert(ds.collect() === Seq(SeqOfRecord(Seq(Record(null, 0, null)))))
  }

  test("reshape - case insensitive") {
    val fixableDF = sparkSession.sql("SELECT 'foo' AS STR, 42 AS nUm, null AS nested")
    val fixedDS = Datasets.reshape[Record](fixableDF)
    assert(fixedDS.collect() === Seq(Record("foo", 42, null)))
  }

  test("reshape - implicits") {
    import io.stoys.core.spark.implicits._
    val df = recordsDF.selectExpr("str", "struct('foo', 'nested str') AS nested", "'foo' AS extra_column")
    val config = ReshapeConfig.dangerous.copy(sortOrder = ReshapeConfig.SortOrder.ALPHABETICAL)
    val ds = df.reshape[Record](config)
    assert(ds.columns === Seq("nested", "num", "str"))
  }
}

object DatasetsTest {
  case class NestedRecord(nestedStr: String)
  case class Record(str: String, num: Int, nested: NestedRecord)
  case class SubsetOfRecord(str: String, nested: NestedRecord)
  case class SeqOfRecord(records: Seq[Record])
}

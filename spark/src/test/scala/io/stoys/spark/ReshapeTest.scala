package io.stoys.spark

import io.stoys.scala.Arbitrary
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.types.{StructField, StructType}

class ReshapeTest extends SparkTestBase {
  import ReshapeTest._
  import sparkSession.implicits._

  private val records = Seq(Record("foo", 42, NestedRecord("nested")))
  private lazy val recordsDF = records.toDF()

  test("coerceTypes") {
    val fixableDF = recordsDF.selectExpr("42 AS str", "CAST(num AS byte) AS num", "nested")
    val fixedDS = Reshape.reshape[Record](fixableDF)
    assert(fixedDS.collect() === Seq(records.head.copy(str = "42")))
    val config = ReshapeConfig.default.copy(coerceTypes = false)
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF, config))
    assert(caught.getMessage.contains("str type IntegerType cannot be casted to target type StringType"))
    assert(caught.getMessage.contains("num type ByteType cannot be casted to target type IntegerType"))
  }

  ignore("conflictResolution") {
    // TODO: implement conflict resolution
  }

  test("dropExtraColumns") {
    val df = recordsDF.selectExpr("*", "'foo' AS extra_column")
    val dsWithoutExtraColumns = Reshape.reshape[Record](df)
    assert(dsWithoutExtraColumns.columns === Seq("str", "num", "nested"))
    val config = ReshapeConfig.default.copy(dropExtraColumns = false)
    val dsWithExtraColumns = Reshape.reshape[Record](df, config)
    assert(dsWithExtraColumns.columns === Seq("str", "num", "nested", "extra_column"))
  }

  test("failOnExtraColumn") {
    val fixableDF = recordsDF.selectExpr("*", "'foo' AS extra_column")
    val fixedDS = Reshape.reshape[Record](fixableDF)
    assert(fixedDS.collect() === records)
    val config = ReshapeConfig.default.copy(failOnExtraColumn = true)
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF, config))
    assert(caught.getMessage.contains("extra_column unexpectedly present"))
  }

  test("failOnIgnoringNullability") {
    val nullableSchema = StructType(recordsDF.schema.fields.map(f => StructField(f.name, f.dataType, nullable = true)))
    val fixableDF = sparkSession.createDataFrame(recordsDF.rdd, nullableSchema)
    val fixedDS = Reshape.reshape[Record](fixableDF)
    assert(fixedDS.collect() === records)
    val config = ReshapeConfig.default.copy(failOnIgnoringNullability = true)
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF, config))
    assert(caught.getMessage.contains("num is nullable but target column is not"))
  }

  test("fillDefaultValues") {
    val fixableDF = sparkSession.sql("SELECT 'unused' AS dummy_column")
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF))
    assert(caught.getMessage.contains("str is missing"))
    assert(caught.getMessage.contains("num is missing"))
    assert(caught.getMessage.contains("nested is missing"))
    val config = ReshapeConfig.default.copy(fillDefaultValues = true)
    val fixedDS = Reshape.reshape[Record](fixableDF, config)
    assert(fixedDS.collect() === Seq(Record("", 0, NestedRecord(""))))
  }

  test("fillMissingNulls") {
    val fixableDF = sparkSession.sql("SELECT 42 AS num")
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF))
    assert(caught.getMessage.contains("str is missing"))
    assert(caught.getMessage.contains("nested is missing"))
    val config = ReshapeConfig.default.copy(fillMissingNulls = true)
    val fixedDS = Reshape.reshape[Record](fixableDF, config)
    assert(fixedDS.collect() === Seq(Record(null, 42, null)))
  }

  test("normalizedNameMatching") {
    val fixableDF = sparkSession.sql("SELECT 'foo' AS `nested str`")
    val caught = intercept[ReshapeException](Reshape.reshape[NestedRecord](fixableDF))
    assert(caught.getMessage.contains("nestedstr is missing"))
    val config = ReshapeConfig.default.copy(normalizedNameMatching = true)
    val fixedDS = Reshape.reshape[NestedRecord](fixableDF, config)
    assert(fixedDS.collect() === Seq(NestedRecord("foo")))
  }

  test("sortOrder") {
    val fixableDF = recordsDF.selectExpr("num", "nested", "str")
    assert(Reshape.reshape[Record](fixableDF).columns === Seq("str", "num", "nested"))
    val sourceOrderConfig = ReshapeConfig.default.copy(sortOrder = ReshapeConfig.SortOrder.SOURCE)
    assert(Reshape.reshape[Record](fixableDF, sourceOrderConfig).columns === Seq("num", "nested", "str"))
    val alphabeticalOrderConfig = ReshapeConfig.default.copy(sortOrder = ReshapeConfig.SortOrder.ALPHABETICAL)
    assert(Reshape.reshape[Record](fixableDF, alphabeticalOrderConfig).columns === Seq("nested", "num", "str"))
  }

  test("arrays") {
    val df = sparkSession.sql("SELECT array(struct('str', 'foo')) AS records")
    val config = ReshapeConfig.dangerous.copy(sortOrder = ReshapeConfig.SortOrder.ALPHABETICAL)
    val ds = Reshape.reshape[SeqOfRecord](df, config)
    assert(ds.collect() === Seq(SeqOfRecord(Seq(Record(null, 0, null)))))
  }

  test("case insensitive") {
    val fixableDF = sparkSession.sql("SELECT 'foo' AS STR, 42 AS nUm, null AS nested")
    val fixedDS = Reshape.reshape[Record](fixableDF)
    assert(fixedDS.collect() === Seq(Record("foo", 42, null)))
  }

  test("ReshapeConfig.as behaves like Arbitrary.empty[ReshapeConfig]") {
    val reshapeConfig = Arbitrary.empty[ReshapeConfig].copy(
      conflictResolution = ReshapeConfig.ConflictResolution.ERROR,
      sortOrder = ReshapeConfig.SortOrder.SOURCE
    )
    assert(reshapeConfig === ReshapeConfig.as)

    val fixableDF = recordsDF.selectExpr("num", "nested", "str")
    val sourceOrderConfig = ReshapeConfig.default.copy(sortOrder = ReshapeConfig.SortOrder.SOURCE)
    val undefinedOrderConfig = ReshapeConfig.default.copy(sortOrder = ReshapeConfig.SortOrder.UNDEFINED)
    assert(Reshape.reshape[Record](fixableDF, undefinedOrderConfig).columns
        === Reshape.reshape[Record](fixableDF, sourceOrderConfig).columns)
  }
}

object ReshapeTest {
  case class NestedRecord(nestedStr: String)
  case class Record(str: String, num: Int, nested: NestedRecord)
  case class SubsetOfRecord(str: String, nested: NestedRecord)
  case class SeqOfRecord(records: Seq[Record])
}

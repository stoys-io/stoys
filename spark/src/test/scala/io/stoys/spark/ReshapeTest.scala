package io.stoys.spark

import io.stoys.scala.Arbitrary
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

import java.nio.file.Files
import java.sql.{Date, Timestamp}

class ReshapeTest extends SparkTestBase {
  import ReshapeTest._
  import sparkSession.implicits._

  private val record = Record("foo", 42, NestedRecord("nested"))
  private val records = Seq(record)
  private lazy val recordsDF = records.toDF()

  test("coerce_types") {
    val fixableDF = recordsDF.selectExpr("42 AS s", "CAST(i AS BYTE) AS i", "nested")
    val fixedDS = Reshape.reshape[Record](fixableDF)
    assert(fixedDS.collect() === Seq(record.copy(s = "42")))
    val config = ReshapeConfig.default.copy(coerce_types = false)
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF, config))
    assert(caught.getMessage.contains("s of type IntegerType cannot be casted to StringType"))
    assert(caught.getMessage.contains("i of type ByteType cannot be casted to IntegerType"))
  }

  test("conflict_resolution") {
    val fixableDF = recordsDF.selectExpr("*", "'second_s' AS s", "'foo' AS extra", "'bar' AS extra")

    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF))
    assert(caught.getMessage.contains("s has 2 conflicting occurrences"))
    val firstConfig = ReshapeConfig.default.copy(conflict_resolution = ReshapeConflictResolution.FIRST)
    assert(Reshape.reshape[Record](fixableDF, firstConfig).collect() === Seq(record))
    val lastConfig = ReshapeConfig.default.copy(conflict_resolution = ReshapeConflictResolution.LAST)
    assert(Reshape.reshape[Record](fixableDF, lastConfig).collect() === Seq(record.copy(s = "second_s")))

    assert(Reshape.reshape[Record](fixableDF, lastConfig).columns.count(_ == "extra") === 0)
    val lastNonDroppingConfig = lastConfig.copy(drop_extra_columns = false)
    assert(Reshape.reshape[Record](fixableDF, lastNonDroppingConfig).columns.count(_ == "extra") === 2)
  }

  test("drop_extra_columns") {
    val df = recordsDF.selectExpr("*", "'foo' AS extra")
    val dsWithoutExtraColumns = Reshape.reshape[Record](df)
    assert(dsWithoutExtraColumns.columns === Seq("s", "i", "nested"))
    val config = ReshapeConfig.default.copy(drop_extra_columns = false)
    val dsWithExtraColumns = Reshape.reshape[Record](df, config)
    assert(dsWithExtraColumns.columns === Seq("s", "i", "nested", "extra"))
  }

  test("fail_on_extra_column") {
    val fixableDF = recordsDF.selectExpr("*", "'foo' AS extra")
    val fixedDS = Reshape.reshape[Record](fixableDF)
    assert(fixedDS.collect() === records)
    val config = ReshapeConfig.default.copy(fail_on_extra_column = true)
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF, config))
    assert(caught.getMessage.contains("extra unexpectedly present"))
  }

  test("fail_on_ignoring_nullability") {
    val nullableSchema = StructType(recordsDF.schema.fields.map(f => StructField(f.name, f.dataType, nullable = true)))
    val fixableDF = sparkSession.createDataFrame(recordsDF.rdd, nullableSchema)
    val fixedDS = Reshape.reshape[Record](fixableDF)
    assert(fixedDS.collect() === records)
    val config = ReshapeConfig.default.copy(fail_on_ignoring_nullability = true)
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF, config))
    assert(caught.getMessage.contains("i is nullable but target column is not"))
  }

  test("fill_default_values") {
    val fixableDF = sparkSession.sql("SELECT 'unused' AS dummy")
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF))
    assert(caught.getMessage.contains("s is missing"))
    assert(caught.getMessage.contains("i is missing"))
    assert(caught.getMessage.contains("nested is missing"))
    val config = ReshapeConfig.default.copy(fill_default_values = true)
    val fixedDS = Reshape.reshape[Record](fixableDF, config)
    assert(fixedDS.collect() === Seq(Record("", 0, NestedRecord(""))))
  }

  test("fill_missing_nulls") {
    val fixableDF = sparkSession.sql("SELECT 42 AS i")
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF))
    assert(caught.getMessage.contains("s is missing"))
    assert(caught.getMessage.contains("nested is missing"))
    val config = ReshapeConfig.default.copy(fill_missing_nulls = true)
    val fixedDS = Reshape.reshape[Record](fixableDF, config)
    assert(fixedDS.collect() === Seq(Record(null, 42, null)))
  }

  test("ReshapeFieldMatchingStrategy.INDEX") {
    val fixableDF = sparkSession.sql("SELECT 'foo' AS _c0")
    val caught = intercept[ReshapeException](Reshape.reshape[NestedRecord](fixableDF))
    assert(caught.getMessage.contains("nestedstring is missing"))
    val config = ReshapeConfig.default.copy(field_matching_strategy = ReshapeFieldMatchingStrategy.INDEX)
    val fixedDS = Reshape.reshape[NestedRecord](fixableDF, config)
    assert(fixedDS.collect() === Seq(NestedRecord("foo")))
  }

  test("ReshapeFieldMatchingStrategy.NAME_NORMALIZED") {
    val fixableDF = sparkSession.sql("SELECT 'foo' AS `nested string`")
    val caught = intercept[ReshapeException](Reshape.reshape[NestedRecord](fixableDF))
    assert(caught.getMessage.contains("nestedstring is missing"))
    val config = ReshapeConfig.default.copy(field_matching_strategy = ReshapeFieldMatchingStrategy.NAME_NORMALIZED)
    val fixedDS = Reshape.reshape[NestedRecord](fixableDF, config)
    assert(fixedDS.collect() === Seq(NestedRecord("foo")))
  }

  test("sort_order") {
    val fixableDF = recordsDF.selectExpr("i", "nested", "s")
    assert(Reshape.reshape[Record](fixableDF).columns === Seq("s", "i", "nested"))
    val sourceOrderConfig = ReshapeConfig.default.copy(sort_order = ReshapeSortOrder.SOURCE)
    assert(Reshape.reshape[Record](fixableDF, sourceOrderConfig).columns === Seq("i", "nested", "s"))
    val alphabeticalOrderConfig = ReshapeConfig.default.copy(sort_order = ReshapeSortOrder.ALPHABETICAL)
    assert(Reshape.reshape[Record](fixableDF, alphabeticalOrderConfig).columns === Seq("i", "nested", "s"))
  }

  test("arrays") {
    val df = sparkSession.sql("SELECT ARRAY(STRUCT(42 AS i, 'foo' AS s)) AS records")
    val config = ReshapeConfig.dangerous.copy(sort_order = ReshapeSortOrder.ALPHABETICAL)
    assert(Reshape.reshape[SeqOfRecord](df, config).collect() === Seq(SeqOfRecord(Seq(Record("foo", 42, null)))))
  }

  ignore("maps") {
    val df = sparkSession.sql("SELECT MAP(0, STRUCT('foo' AS s, 42 AS i)) AS records")
    val config = ReshapeConfig.dangerous.copy(sort_order = ReshapeSortOrder.ALPHABETICAL)
    assert(Reshape.reshape[MapOfRecord](df, config).collect() === Seq(MapOfRecord(Map("0" -> Record("foo", 42, null)))))
  }

  test("custom temporal formats - DpConfig") {
    val fixableDF = sparkSession.sql("SELECT '02/20/2020' AS date, '02/20/2020 02:20' AS timestamp")
    assert(Reshape.reshape[TemporalRecord](fixableDF).collect() === Seq(TemporalRecord(null, null)))
    val config = ReshapeConfig.default.copy(
      date_format = Some("MM/dd/yyyy"), timestamp_format = Some("MM/dd/yyyy HH:mm"))
    val fixedDS = Reshape.reshape[TemporalRecord](fixableDF, config)
    assert(fixedDS.collect()
        === Seq(TemporalRecord(Date.valueOf("2020-02-20"), Timestamp.valueOf("2020-02-20 02:20:00"))))
  }

  test("custom temporal formats - Metadata") {
    val fixableDF = sparkSession.sql("SELECT '02/20/2020' AS date, '02/20/2020 02:20' AS timestamp")
    val defaultSchema = ScalaReflection.schemaFor[TemporalRecord].dataType
    val targetSchema = StructType(Seq(
      StructField("date", DateType, metadata = Metadata.fromJson("""{"format": "MM/dd/yyyy"}""")),
      StructField("timestamp", TimestampType, metadata = Metadata.fromJson("""{"format": "MM/dd/yyyy HH:mm"}"""))
    ))
    val reshapedToDefaultSchema = Reshape.reshapeToDF(fixableDF, defaultSchema).as[TemporalRecord]
    assert(reshapedToDefaultSchema.collect() === Seq(TemporalRecord(null, null)))
    val reshapedToTargetSchema = Reshape.reshapeToDF(fixableDF, targetSchema).as[TemporalRecord]
    assert(reshapedToTargetSchema.collect()
        === Seq(TemporalRecord(Date.valueOf("2020-02-20"), Timestamp.valueOf("2020-02-20 02:20:00"))))
  }

  test("custom enum values - Metadata") {
    val fixableDF = sparkSession.sql("SELECT 'Bar ' AS enumeration")
    val defaultSchema = ScalaReflection.schemaFor[EnumerationRecord].dataType
    val fooBarBazMetadata = Metadata.fromJson("""{"enum_values": ["foo", "bar", "baz"]}""")
    val targetSchema = StructType(Seq(
      StructField("enumeration", IntegerType, metadata = fooBarBazMetadata)
    ))
    val reshapedToDefaultSchema = Reshape.reshapeToDF(fixableDF, defaultSchema).as[EnumerationRecord]
    assert(reshapedToDefaultSchema.collect() === Seq(EnumerationRecord(null)))
    val reshapedToTargetSchema = Reshape.reshapeToDF(fixableDF, targetSchema).as[EnumerationRecord]
    assert(reshapedToTargetSchema.collect() === Seq(EnumerationRecord(1)))
  }

  test("case insensitive") {
    val fixableDF = sparkSession.sql("SELECT 'foo' AS S, 42 AS I, NULL AS nested")
    val fixedDS = Reshape.reshape[Record](fixableDF)
    assert(fixedDS.collect() === Seq(Record("foo", 42, null)))
  }

  test("ReshapeConfig.as behaves like Arbitrary.empty[ReshapeConfig]") {
    val reshapeConfig = Arbitrary.empty[ReshapeConfig].copy(
      conflict_resolution = ReshapeConflictResolution.ERROR,
      field_matching_strategy = ReshapeFieldMatchingStrategy.NAME_DEFAULT,
      sort_order = ReshapeSortOrder.SOURCE
    )
    assert(reshapeConfig === ReshapeConfig.as)

    val fixableDF = recordsDF.selectExpr("i", "nested", "s")
    val sourceOrderConfig = ReshapeConfig.default.copy(sort_order = ReshapeSortOrder.SOURCE)
    val undefinedOrderConfig = ReshapeConfig.default.copy(sort_order = ReshapeSortOrder.UNDEFINED)
    assert(Reshape.reshape[Record](fixableDF, undefinedOrderConfig).columns
        === Reshape.reshape[Record](fixableDF, sourceOrderConfig).columns)
  }

  test("reshape cannot fix json inference behaviour resolving empty array as array of strings") {
    val emptySeqOfRecordsJsonPath = tmpDir.resolve("empty_seq_of_records.json")
    Files.write(emptySeqOfRecordsJsonPath, "{\"records\": []}".getBytes)
    val fixableDF = sparkSession.read.format("json").load(emptySeqOfRecordsJsonPath.toString)
    // This is the issue. Should spark infer something else? ArrayType(NullType) maybe?
    assert(fixableDF.schema.fields.head.dataType === ArrayType(StringType))
    val caught = intercept[ReshapeException](Reshape.reshape[SeqOfRecord](fixableDF, ReshapeConfig.dangerous))
    assert(caught.getMessage.contains("Column records[] of type StringType cannot be casted"))
  }
}

object ReshapeTest {
  case class NestedRecord(nestedString: String)
  case class Record(s: String, i: Int, nested: NestedRecord)
  case class SubsetOfRecord(s: String, nested: NestedRecord)
  case class SeqOfRecord(records: Seq[Record])
  case class MapOfRecord(records: Map[String, Record])
  case class TemporalRecord(date: Date, timestamp: Timestamp)
  case class EnumerationRecord(enumeration: java.lang.Integer)
}

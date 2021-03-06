package io.stoys.spark

import io.stoys.scala.Arbitrary
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, Metadata, StringType, StructField, StructType, TimestampType}

import java.nio.file.Files
import java.sql.{Date, Timestamp}

class ReshapeTest extends SparkTestBase {
  import ReshapeTest._
  import sparkSession.implicits._

  private val record = Record("foo", 42, NestedRecord("nested"))
  private val records = Seq(record)
  private lazy val recordsDF = records.toDF()

  test("coerceTypes") {
    val fixableDF = recordsDF.selectExpr("42 AS str", "CAST(num AS BYTE) AS num", "nested")
    val fixedDS = Reshape.reshape[Record](fixableDF)
    assert(fixedDS.collect() === Seq(record.copy(str = "42")))
    val config = ReshapeConfig.default.copy(coerceTypes = false)
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF, config))
    assert(caught.getMessage.contains("str of type IntegerType cannot be casted to StringType"))
    assert(caught.getMessage.contains("num of type ByteType cannot be casted to IntegerType"))
  }

  test("conflictResolution") {
    val fixableDF = recordsDF.selectExpr("*", "'second_str' AS str", "'foo' AS extra", "'bar' AS extra")

    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF))
    assert(caught.getMessage.contains("str has 2 conflicting occurrences"))
    val firstConfig = ReshapeConfig.default.copy(conflictResolution = ReshapeConflictResolution.FIRST)
    assert(Reshape.reshape[Record](fixableDF, firstConfig).collect() === Seq(record))
    val lastConfig = ReshapeConfig.default.copy(conflictResolution = ReshapeConflictResolution.LAST)
    assert(Reshape.reshape[Record](fixableDF, lastConfig).collect() === Seq(record.copy(str = "second_str")))

    assert(Reshape.reshape[Record](fixableDF, lastConfig).columns.count(_ == "extra") === 0)
    val lastNonDroppingConfig = lastConfig.copy(dropExtraColumns = false)
    assert(Reshape.reshape[Record](fixableDF, lastNonDroppingConfig).columns.count(_ == "extra") === 2)
  }

  test("dropExtraColumns") {
    val df = recordsDF.selectExpr("*", "'foo' AS extra")
    val dsWithoutExtraColumns = Reshape.reshape[Record](df)
    assert(dsWithoutExtraColumns.columns === Seq("str", "num", "nested"))
    val config = ReshapeConfig.default.copy(dropExtraColumns = false)
    val dsWithExtraColumns = Reshape.reshape[Record](df, config)
    assert(dsWithExtraColumns.columns === Seq("str", "num", "nested", "extra"))
  }

  test("failOnExtraColumn") {
    val fixableDF = recordsDF.selectExpr("*", "'foo' AS extra")
    val fixedDS = Reshape.reshape[Record](fixableDF)
    assert(fixedDS.collect() === records)
    val config = ReshapeConfig.default.copy(failOnExtraColumn = true)
    val caught = intercept[ReshapeException](Reshape.reshape[Record](fixableDF, config))
    assert(caught.getMessage.contains("extra unexpectedly present"))
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
    val fixableDF = sparkSession.sql("SELECT 'unused' AS dummy")
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
    val sourceOrderConfig = ReshapeConfig.default.copy(sortOrder = ReshapeSortOrder.SOURCE)
    assert(Reshape.reshape[Record](fixableDF, sourceOrderConfig).columns === Seq("num", "nested", "str"))
    val alphabeticalOrderConfig = ReshapeConfig.default.copy(sortOrder = ReshapeSortOrder.ALPHABETICAL)
    assert(Reshape.reshape[Record](fixableDF, alphabeticalOrderConfig).columns === Seq("nested", "num", "str"))
  }

  test("arrays") {
    val df = sparkSession.sql("SELECT ARRAY(STRUCT(42 AS num, 'foo' AS str)) AS records")
    val config = ReshapeConfig.dangerous.copy(sortOrder = ReshapeSortOrder.ALPHABETICAL)
    assert(Reshape.reshape[SeqOfRecord](df, config).collect() === Seq(SeqOfRecord(Seq(Record("foo", 42, null)))))
  }

  ignore("maps") {
    val df = sparkSession.sql("SELECT MAP(0, STRUCT('foo' AS str, 42 AS num)) AS records")
    val config = ReshapeConfig.dangerous.copy(sortOrder = ReshapeSortOrder.ALPHABETICAL)
    assert(Reshape.reshape[MapOfRecord](df, config).collect() === Seq(MapOfRecord(Map("0" -> Record("foo", 42, null)))))
  }

  test("custom temporal formats - DpConfig") {
    val fixableDF = sparkSession.sql("SELECT '02/20/2020' AS date, '02/20/2020 02:20' AS timestamp")
    assert(Reshape.reshape[TemporalRecord](fixableDF).collect() === Seq(TemporalRecord(null, null)))
    val config = ReshapeConfig.default.copy(dateFormat = Some("MM/dd/yyyy"), timestampFormat = Some("MM/dd/yyyy HH:mm"))
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
    val fixableDF = sparkSession.sql("SELECT 'foo' AS STR, 42 AS nUm, NULL AS nested")
    val fixedDS = Reshape.reshape[Record](fixableDF)
    assert(fixedDS.collect() === Seq(Record("foo", 42, null)))
  }

  test("ReshapeConfig.as behaves like Arbitrary.empty[ReshapeConfig]") {
    val reshapeConfig = Arbitrary.empty[ReshapeConfig].copy(
      conflictResolution = ReshapeConflictResolution.ERROR,
      sortOrder = ReshapeSortOrder.SOURCE
    )
    assert(reshapeConfig === ReshapeConfig.as)

    val fixableDF = recordsDF.selectExpr("num", "nested", "str")
    val sourceOrderConfig = ReshapeConfig.default.copy(sortOrder = ReshapeSortOrder.SOURCE)
    val undefinedOrderConfig = ReshapeConfig.default.copy(sortOrder = ReshapeSortOrder.UNDEFINED)
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
    assert(caught.getMessage.contains("Column null of type StringType cannot be casted to StructType"))
  }
}

object ReshapeTest {
  case class NestedRecord(nestedStr: String)
  case class Record(str: String, num: Int, nested: NestedRecord)
  case class SubsetOfRecord(str: String, nested: NestedRecord)
  case class SeqOfRecord(records: Seq[Record])
  case class MapOfRecord(records: Map[String, Record])
  case class TemporalRecord(date: Date, timestamp: Timestamp)
  case class EnumerationRecord(enumeration: java.lang.Integer)
}

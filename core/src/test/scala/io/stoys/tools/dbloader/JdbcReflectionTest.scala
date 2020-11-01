package io.stoys.tools.dbloader

import java.sql.Date

import io.stoys.core.spark.TableName
import org.scalatest.funsuite.AnyFunSuite

class JdbcReflectionTest extends AnyFunSuite {
  import JdbcReflection._
  import JdbcReflectionTest._

  test("getCreateTableStatement") {
    val caughtNoEntity =
      intercept[IllegalArgumentException](getCreateTableStatement(TableName[RecordWithNoEntity], null))
    assert(caughtNoEntity.getMessage.contains("RecordWithNoEntity is not annotated with javax.persistence.Entity"))
    assert(getCreateTableStatement(TableName[RecordWithNoId], null) ===
        """CREATE TABLE record_with_no_id (
          |  value INT NOT NULL
          |);""".stripMargin)
    assert(getCreateTableStatement(TableName[RecordWithId], null) ===
        """CREATE TABLE record_with_id (
          |  id INT NOT NULL
          |);""".stripMargin)
    assert(getCreateTableStatement(TableName[RecordWithMultipleIds], null) ===
        """CREATE TABLE record_with_multiple_ids (
          |  id1 INT NOT NULL,
          |  id2 INT NOT NULL
          |);""".stripMargin)
    assert(getCreateTableStatement(TableName[RecordWithPrimitives], null) ===
        """CREATE TABLE record_with_primitives (
          |  boolean_field BOOLEAN NOT NULL,
          |  int_field INT NOT NULL,
          |  long_field BIGINT NOT NULL,
          |  float_field FLOAT NOT NULL,
          |  double_field DOUBLE NOT NULL,
          |  optional BIGINT,
          |  precision_and_scale BIGINT NOT NULL
          |);""".stripMargin)
    assert(getCreateTableStatement(TableName[RecordWithStrings], null) ===
        """CREATE TABLE record_with_strings (
          |  id VARCHAR(255) NOT NULL,
          |  optional VARCHAR(255),
          |  no_annotation VARCHAR(255) NOT NULL,
          |  lob_annotation TEXT NOT NULL,
          |  name_is_ignored VARCHAR(255) NOT NULL,
          |  nullable_is_ignored VARCHAR(255) NOT NULL,
          |  length VARCHAR(42) NOT NULL,
          |  column_definition CHAR(42),
          |  unique VARCHAR(255) NOT NULL,
          |  camel_cased VARCHAR(255) NOT NULL,
          |  snake_cased VARCHAR(255) NOT NULL
          |);""".stripMargin)
    assert(getCreateTableStatement(TableName[RecordWithDate], null) ===
        """CREATE TABLE record_with_date (
          |  date DATE NOT NULL
          |);""".stripMargin)
    assert(getCreateTableStatement(TableName[RecordWithCollections], null) ===
        """CREATE TABLE record_with_collections (
          |  seq JSON NOT NULL,
          |  map JSON NOT NULL
          |);""".stripMargin)
    assert(getCreateTableStatement(TableName[RecordWithNestedRecord], null) ===
        """CREATE TABLE record_with_nested_record (
          |  nested JSON NOT NULL
          |);""".stripMargin)
    val caughtUnsupportedType =
      intercept[IllegalArgumentException](getCreateTableStatement(TableName[RecordWithUnsupportedType], null))
    assert(caughtUnsupportedType.getMessage.contains("Unsupported type IOException!"))

    assert(getCreateTableStatement(TableName[RecordWithNoId], "schema_name") ===
        """CREATE TABLE schema_name.record_with_no_id (
          |  value INT NOT NULL
          |);""".stripMargin)
  }

  test("getAddConstraintStatements") {
    val caughtNoEntity =
      intercept[IllegalArgumentException](getAddConstraintStatements(TableName[RecordWithNoEntity], null))
    assert(caughtNoEntity.getMessage.contains("RecordWithNoEntity is not annotated with javax.persistence.Entity"))
    assert(getAddConstraintStatements(TableName[RecordWithNoId], null) === Seq.empty)
    assert(getAddConstraintStatements(TableName[RecordWithId], null)
        === Seq("ALTER TABLE record_with_id ADD PRIMARY KEY (id);"))
    assert(getAddConstraintStatements(TableName[RecordWithMultipleIds], null)
        === Seq("ALTER TABLE record_with_multiple_ids ADD PRIMARY KEY (id1, id2);"))

    assert(getAddConstraintStatements(TableName[RecordWithId], "schema_name")
        === Seq("ALTER TABLE schema_name.record_with_id ADD PRIMARY KEY (id);"))
    assert(getAddConstraintStatements(TableName[RecordWithStrings], "schema_name") === Seq(
      "ALTER TABLE schema_name.record_with_strings ADD CONSTRAINT record_with_strings_unique UNIQUE (unique);",
      "ALTER TABLE schema_name.record_with_strings ADD PRIMARY KEY (id);"))
  }
}

object JdbcReflectionTest {
  import javax.persistence._
//  import ScalaPersistenceAnnotations._

  case class RecordWithNoEntity(
      @Id
      id: Int)

  @Entity
  case class RecordWithNoId(
      value: Int)

  @Entity
  case class RecordWithId(
      @Id
      id: Int)

  @Entity
  case class RecordWithMultipleIds(
      @Id
      id1: Int,
      @Id
      id2: Int)

  @Entity
  case class RecordWithPrimitives(
      booleanField: Boolean,
      intField: Int,
      longField: Long,
      floatField: Float,
      doubleField: Double,
      optional: Option[Long],
      @Column(precision = 18, scale = 2)
      precisionAndScale: Long)

  @Entity
  case class RecordWithStrings(
      @Id
      id: String,
      optional: Option[String],
      noAnnotation: String,
      @Lob
      lobAnnotation: String,
      @Column(name = "ignored_name")
      nameIsIgnored: String,
      @Column(nullable = true)
      nullableIsIgnored: String,
      @Column(length = 42)
      length: String,
      @Column(columnDefinition = "CHAR(42)")
      columnDefinition: String,
      @Column(unique = true)
      unique: String,
      camelCased: String,
      snake_cased: String)

  @Entity
  case class RecordWithDate(
      date: Date)

  @Entity
  case class RecordWithCollections(
      seq: Seq[String],
      map: Map[String, String])

  case class NestedRecord(
      value: String)

  @Entity
  case class RecordWithNestedRecord(
      nested: NestedRecord)

  @Entity
  case class RecordWithUnsupportedType(
      nested: java.io.IOException)
}

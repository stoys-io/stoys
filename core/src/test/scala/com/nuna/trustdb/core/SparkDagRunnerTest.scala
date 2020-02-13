package com.nuna.trustdb.core

import java.nio.file.Files
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.nuna.trustdb.core.sql.SparkSqlRunner
import com.nuna.trustdb.core.util.Configuration
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}

import scala.io.Source
import scala.reflect.runtime.universe._

class SparkDagRunnerTest extends SparkTestBase {
  import SparkDagRunnerTest._
  import sparkSession.implicits._

  test("SparkDagRunner") {
    val testDir = Files.createTempDirectory(this.getClass.getSimpleName + ".").toFile
    testDir.deleteOnExit()

    val runTimestamp = "20191104125146"
    val inputPath = s"$testDir/ingestion"
    val outputPath = s"$testDir/output"
    val publishPath = s"$testDir/shared"
    val inputListFile = s"$outputPath/input.list"
    val outputListFile = s"$outputPath/io.list"
    val sharedOutputListFile = s"$publishPath/latest.io.list"

    writeTestData(Seq(Foo(1, 11), Foo(2, 21)), s"$inputPath/foo")
    writeTestData(Seq(Bar(1, 12), Bar(2, 22)), s"$inputPath/bar")

    val sparkArgs = Array(
      s"spark_dag_runner_config@@main_dag_class=${classOf[MainDag].getName}",
      s"spark_dag_runner_config@@run_timestamp=$runTimestamp",
      s"spark_dag_runner_config@@compute_collections@@0=pack",
      s"spark_io_config@@input_paths@@0=$inputPath?sos-listing_strategy=@",
      s"spark_io_config@@output_path=$outputPath",
      s"spark_io_config@@shared_output_path=$publishPath"
    )
    val insightArgs = Array(
      "pack_insight_params@@add_multiplier=1",
      "pack_insight_params@@multiply_multiplier=1000")
    SparkDagRunner.main(Array("--environments=dev") ++ sparkArgs ++ insightArgs)

    val actual = readTestResult[Pack](s"$outputPath/pack")
    val expected = Seq(Pack(1, 1000 * 11 * 12 + 11 + 12), Pack(2, 1000 * 21 * 22 + 21 + 22)).toDS()
    assertDatasetEquality(actual, expected, ignoreNullable = true)

    val actualMetrics = sparkSession.read.format("delta").load(s"$publishPath/metric")
    val ts = Timestamp.valueOf(LocalDateTime.parse(runTimestamp, DateTimeFormatter.ofPattern("yyyyMMddHHmmss")))
    val expectedMetrics = Array(
      Row("add_metric_1", 1.0, Map.empty[String, String], ts),
      Row("add_metric_2", 2.0, Map("k1" -> "v1"), ts))
    assert(actualMetrics.orderBy(desc("run_timestamp"), asc("name")).collect() === expectedMetrics)

    val actualInputLists = Source.fromFile(inputListFile).getLines.toList
    val expectedInputs = List(s"file:$inputPath/bar?sos-format=parquet&sos-table_name=bar",
      s"file:$inputPath/foo?sos-format=parquet&sos-table_name=foo")
    assert(actualInputLists === expectedInputs)

    val actualOutputLists = Source.fromFile(outputListFile).getLines.filterNot(_.startsWith("#")).toList
    val sharedOutputLists = Source.fromFile(sharedOutputListFile).getLines.filterNot(_.startsWith("#")).toList
    val expectedOutputs = List(s"$outputPath/pack?sos-format=parquet&sos-table_name=pack",
      s"$outputPath/metric?sos-format=parquet&sos-table_name=metric") ++ expectedInputs
    assert(actualOutputLists === expectedOutputs)
    assert(sharedOutputLists === expectedOutputs)
  }

  private def writeTestData[T: TypeTag : Encoder](data: Seq[T], path: String): Unit = {
    data.toDS().write.format("parquet").mode("overwrite").save(path)
  }

  private def readTestResult[T: TypeTag : Encoder](path: String): Dataset[T] = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    sparkSession.read.format("parquet").schema(schema).load(path).as[T]
  }
}

object SparkDagRunnerTest {
  case class Foo(id: Int, value: Int)
  case class Bar(id: Int, value: Int)
  case class Add(id: Int, value: Int)
  case class Multiply(id: Int, value: Int)
  case class Pack(id: Int, value: Long)

  class AddInsight(sparkSession: SparkSession) {
    import sparkSession.implicits._

    def add(foo: Dataset[Foo], bar: Dataset[Bar]): Dataset[Add] = {
      SparkSqlRunner.runSql(sparkSession, this.getClass, "add.sql", Map("foo" -> foo, "bar" -> bar))
    }

    def metrics(add: Dataset[Add]): Dataset[Metric] = {
      Seq(Metric("add_metric_1", 1.0, Map.empty), Metric("add_metric_2", 2.0, Map("k1" -> "v1"))).toDS()
    }
  }

  case class FooJoinBar(id: Int, fooValue: Int, barValue: Int)

  class MultiplyInsight(sparkSession: SparkSession) {
    import sparkSession.implicits._

    def multiply(foo: Dataset[Foo], bar: Dataset[Bar]): Dataset[Multiply] = {
      //      SparkSqlRunner.runSql(sparkSession, this.getClass, "multiply.sql", Map("foo" -> foo, "bar" -> bar))
      val nonCollidingFoo = foo.withColumnRenamed("value", "fooValue")
      val nonCollidingBar = bar.withColumnRenamed("value", "barValue")
      val joined = nonCollidingFoo.join(nonCollidingBar, "id").as[FooJoinBar]

      joined.map(x => Multiply(x.id, x.fooValue * x.barValue))
    }
  }

  case class PackInsightParams(add_multiplier: Long, multiply_multiplier: Long)

  class PackInsight(sparkSession: SparkSession, params: PackInsightParams) {
    def pack(add: Dataset[Add], multiply: Dataset[Multiply]): Dataset[Pack] = {
      val inputs = Map("add" -> add, "multiply" -> multiply)
      SparkSqlRunner.runSql(sparkSession, this.getClass, "pack.sql", inputs, Some(params))
    }
  }

  class IngestionInsight(sparkDagRunner: SparkDagRunner) {
    def foo(): Dataset[Foo] = sparkDagRunner.read[Foo]

    def bar(): Dataset[Bar] = sparkDagRunner.read[Bar]
  }

  class PackDag(configuration: Configuration, session: SparkSession, runner: SparkDagRunner) {
    def pack(foo: Dataset[Foo], bar: Dataset[Bar]): Dataset[Pack] = {
      val addInsight = new AddInsight(session)
      val add = runner.wrap(addInsight.add(foo, bar))
      runner.wrapMetrics(addInsight.metrics(add))
      val multiplyInsight = new MultiplyInsight(session)
      val multiply = runner.wrap(multiplyInsight.multiply(foo, bar))
      new PackInsight(session, configuration.readConfig[PackInsightParams]).pack(add, multiply)
    }
  }

  class MainDag(configuration: Configuration, session: SparkSession, runner: SparkDagRunner) extends Runnable {
    def run(): Unit = {
      val ingestionInsight = new IngestionInsight(runner)
      val foo = runner.wrap(ingestionInsight.foo())
      val bar = runner.wrap(ingestionInsight.bar())
      runner.wrap(new PackDag(configuration, session, runner).pack(foo, bar))
    }
  }
}

package io.stoys.spark

import java.sql.Timestamp
import java.time.LocalDateTime

import io.stoys.scala.Configuration
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source

class SparkDagRunnerTest extends SparkTestBase {
  import SparkDagRunnerTest._
  import sparkSession.implicits._

  test("SparkDagRunner") {
    val runTimestamp = "2020-02-02T02:22:20"
    val inputPath = s"${tmpDir.toAbsolutePath}/ingestion"
    val outputPath = s"${tmpDir.toAbsolutePath}/output"
    val sharedOutputPath = s"${tmpDir.toAbsolutePath}/shared_output_path"

    writeData(s"$inputPath/foo", Seq(Foo(1, 11), Foo(2, 21)))
    writeData(s"$inputPath/bar", Seq(Bar(1, 12), Bar(2, 22)))

    val sparkDagRunnerArgs = Array(
      s"spark_dag_runner_config__main_dag_class=${classOf[MainDag].getName}",
      s"spark_dag_runner_config__run_timestamp=$runTimestamp",
      s"spark_dag_runner_config__compute_collections__0=pack",
      s"spark_dag_runner_config__shared_output_path=$sharedOutputPath",
      s"spark_io_config__input_paths__0=$inputPath?sos-listing_strategy=tables",
      s"spark_io_config__output_path=$outputPath"
    )
    val insightArgs = Array(
      "pack_insight_params__add_multiplier=1",
      "pack_insight_params__multiply_multiplier=1000")
    SparkDagRunner.main(Array("--environments=dev") ++ sparkDagRunnerArgs ++ insightArgs)

    val expectedPack = Seq(Pack(1, 1000 * 11 * 12 + 11 + 12), Pack(2, 1000 * 21 * 22 + 21 + 22))
    assert(readDataset[Pack](s"$outputPath/pack").collect() === expectedPack)

    val sharedMetrics = sparkSession.read.format("delta").load(s"$sharedOutputPath/metric")
    val ts = Timestamp.valueOf(LocalDateTime.parse(runTimestamp))
    assert(sharedMetrics.collect() === Array(Row("add_metric", 42.0, Map("foo" -> "bar"), ts)))

    assert(Source.fromFile(s"$outputPath/.dag/input_tables.list").getLines.toSet
        === Set(s"file:$inputPath/bar?sos-table_name=bar", s"file:$inputPath/foo?sos-table_name=foo"))
    assert(Source.fromFile(s"$outputPath/.dag/output_tables.list").getLines.toSet === Set(
      s"$outputPath/pack?sos-format=parquet&sos-table_name=pack",
      s"$outputPath/metric?sos-format=parquet&sos-table_name=metric"))
    assert(Source.fromFile(s"$sharedOutputPath/latest.list").getLines.toSet
      === Set(s"$outputPath?sos-listing_strategy=dag"))
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
      Seq(Metric("add_metric", 42.0, Map("foo" -> "bar"))).toDS()
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

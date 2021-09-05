package io.stoys.spark

import io.stoys.scala.{Arbitrary, IO}
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField}

class SparkIOTest extends SparkTestBase {
  import SparkIOTest._

  private lazy val dfs = Dfs(sparkSession)

  private lazy val emptySparkIOConfig = Arbitrary.empty[SparkIOConfig]

  test("addInputPath") {
    InputPathResolverTest.writeTmpDagLists(dfs, tmpDir, "dag", Seq.empty, Seq.empty, Seq("dag/foo"))
    writeTmpData("dag/foo", Seq.empty[Option[Int]])
    writeTmpData("non_dag_table", Seq.empty[Option[Int]])

    val config = emptySparkIOConfig.copy(input_paths = Seq(s"$tmpDir/dag/.dag"), output_path = Some(s"$tmpDir/out"))
    IO.using(new SparkIO(sparkSession, config)) { sparkIO =>
      assert(sparkIO.getInputTable("foo").isDefined)

      assert(sparkIO.getInputTable("non_dag_table").isEmpty)
      sparkIO.addInputPath(s"$tmpDir/non_dag_table")
      assert(sparkIO.getInputTable("non_dag_table").isDefined)

      assert(sparkIO.getInputTable("bar").isEmpty)
      sparkIO.addInputPath(s"$tmpDir/non_dag_table?sos-table_name=bar")
      assert(sparkIO.getInputTable("bar").isDefined)

      val conflictingException = intercept[SToysException](sparkIO.addInputPath(s"$tmpDir/dag/foo?sos-table_name=bar"))
      assert(conflictingException.getMessage.contains("conflicting tables"))

      // It is fine to add path that will resolve to the same table (including all options).
      sparkIO.addInputPath(s"$tmpDir/dag/foo?sos-table_name=foo")

      assert(!dfs.exists(s"$tmpDir/out/.dag"))
    }

    assert(dfs.exists(s"$tmpDir/out/.dag"))
  }

  test("SparkIOConfig.register_input_tables") {
    writeTmpData("record", Seq.empty[Record])

    val config = emptySparkIOConfig.copy(input_paths = Seq(s"$tmpDir/record"))
    IO.using(new SparkIO(sparkSession, config)) { sparkIO =>
      assert(Datasets.getAlias(sparkIO.ds(TableName[Record])) === Some("record"))
      assert(!sparkSession.catalog.tableExists("record"))

      assert(!sparkSession.catalog.tableExists("record__renamed"))
      sparkIO.addInputPath(s"$tmpDir/record?sos-table_name=record__renamed")
      assert(!sparkSession.catalog.tableExists("record__renamed"))
    }

    IO.using(new SparkIO(sparkSession, config.copy(register_input_tables = true))) { sparkIO =>
      assert(Datasets.getAlias(sparkIO.ds(TableName[Record])) === Some("record"))
      assert(sparkSession.catalog.tableExists("record"))

      assert(!sparkSession.catalog.tableExists("record__renamed"))
      sparkIO.addInputPath(s"$tmpDir/record?sos-table_name=record__renamed")
      assert(sparkSession.catalog.tableExists("record__renamed"))

      assert(Datasets.getAlias(sparkIO.ds(TableName[Record]("renamed"))) === Some("record__renamed"))
      assert(Datasets.getAlias(sparkSession.table("record__renamed")) === Some("record__renamed"))
    }
  }

  test("reshape_config") {
    writeTmpData("record", Seq(Record(42)))
    val recordConfig = emptySparkIOConfig.copy(input_paths = Seq(s"$tmpDir/record"))
    IO.using(new SparkIO(sparkSession, recordConfig)) { sparkIO =>
      assert(sparkIO.ds(TableName[Record]).schema.fields === Array(StructField("i", IntegerType)))
    }

    writeTmpData("different_record", Seq(DifferentConfig(42)))
    val badRecordConfig = emptySparkIOConfig.copy(
      input_paths = Seq(
        s"$tmpDir/different_record?sos-table_name=record&sos-reshape_config__field_matching_strategy=INDEX"),
      input_reshape_config = ReshapeConfig.default
    )
    IO.using(new SparkIO(sparkSession, badRecordConfig)) { sparkIO =>
      assert(sparkIO.df(TableName[Record]).schema.fields === Array(StructField("foo", DoubleType)))
      assert(sparkIO.ds(TableName[Record]).schema.fields === Array(StructField("i", IntegerType)))
      assert(!sparkSession.catalog.tableExists("different_record"))
    }
  }
}

object SparkIOTest {
  case class Record(i: Int)
  case class DifferentConfig(foo: Double)
}

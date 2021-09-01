package io.stoys.spark

import io.stoys.scala.{Arbitrary, IO}
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.AnalysisException

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

  test("SparkIOConfig.registerInputTables") {
    writeTmpData("record", Seq.empty[Record])

    val config = emptySparkIOConfig.copy(input_paths = Seq(s"$tmpDir/record"), output_path = Some(s"$tmpDir/out"))
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
}

object SparkIOTest {
  case class Record(s: String)
}

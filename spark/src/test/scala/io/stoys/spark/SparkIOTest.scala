package io.stoys.spark

import io.stoys.scala.{Arbitrary, IO}
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.AnalysisException

class SparkIOTest extends SparkTestBase {
  import SparkIOTest._

  private lazy val dfs = Dfs(sparkSession)

  private lazy val emptySparkIOConfig = Arbitrary.empty[SparkIOConfig]

  test("addInputPaths") {
    InputPathResolverTest.writeTmpDagLists(dfs, tmpDir, "dag", Seq.empty, Seq.empty, Seq("dag/foo"))
    writeTmpData("dag/foo", Seq.empty[Option[Int]])
    writeTmpData("non_dag_table", Seq.empty[Option[Int]])

    val sparkIOConfig = emptySparkIOConfig.copy(inputPaths = Seq(s"$tmpDir/dag/.dag"), outputPath = Some(s"$tmpDir/out"))
    IO.using(new SparkIO(sparkSession, sparkIOConfig)) { sparkIO =>
      assert(sparkIO.getInputTable("foo").isEmpty)
      sparkIO.init()
      assert(sparkIO.getInputTable("foo").isDefined)

      assert(sparkIO.getInputTable("non_dag_table").isEmpty)
      sparkIO.addInputPaths(s"$tmpDir/non_dag_table")
      assert(sparkIO.getInputTable("non_dag_table").isDefined)

      assert(sparkIO.getInputTable("bar").isEmpty)
      sparkIO.addInputPaths(s"$tmpDir/non_dag_table?sos-table_name=bar")
      assert(sparkIO.getInputTable("bar").isDefined)

      val conflictingException = intercept[SToysException](sparkIO.addInputPaths(s"$tmpDir/dag/foo?sos-table_name=bar"))
      assert(conflictingException.getMessage.contains("conflicting tables"))

      // It is fine to add path that will resolve to the same table (including all options).
      sparkIO.addInputPaths(s"$tmpDir/dag/foo?sos-table_name=foo")

      assert(!dfs.exists(s"$tmpDir/out/.dag"))
    }

    assert(dfs.exists(s"$tmpDir/out/.dag"))
  }

  test("addInputPaths - Datasets.getAlias") {
    writeTmpData("record", Seq.empty[Record])

    val sparkIOConfig = emptySparkIOConfig.copy(inputPaths = Seq(s"$tmpDir/record"), outputPath = Some(s"$tmpDir/out"))
    IO.using(new SparkIO(sparkSession, sparkIOConfig)) { sparkIO =>
      intercept[AnalysisException](sparkSession.table("record"))
      sparkIO.init()
      assert(Datasets.getAlias(sparkSession.table("record")) === Some("record"))
      assert(Datasets.getAlias(sparkIO.ds(TableName[Record])) === Some("record"))

      intercept[AnalysisException](sparkSession.table("record__renamed"))
      sparkIO.addInputPaths(s"$tmpDir/record?sos-table_name=record__renamed")
      assert(Datasets.getAlias(sparkSession.table("record__renamed")) === Some("record__renamed"))
      assert(Datasets.getAlias(sparkIO.ds(TableName[Record]("renamed"))) === Some("record__renamed"))
    }
  }
}

object SparkIOTest {
  case class Record(s: String)
}

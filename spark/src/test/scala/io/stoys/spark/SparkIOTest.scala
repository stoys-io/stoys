package io.stoys.spark

import io.stoys.scala.{Arbitrary, IO}
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.AnalysisException

class SparkIOTest extends SparkTestBase {
  import SparkIO._
  import SparkIOTest._

  private lazy val dfs = Dfs(sparkSession)

  private lazy val emptySparkIOConfig = Arbitrary.empty[SparkIOConfig]
  private lazy val emptySosTable = Arbitrary.empty[SosTable]

  test("parseInputPath") {
    val emptySosOptions = Arbitrary.empty[SosOptions]
    assert(parseInputPath("dir") === ParsedInputPath("dir", emptySosOptions, Map.empty))
    assert(parseInputPath("dir/file") === ParsedInputPath("dir/file", emptySosOptions, Map.empty))
    assert(parseInputPath("?param=value") === ParsedInputPath("", emptySosOptions, Map("param" -> "value")))
    assert(parseInputPath("dir/file?param1=value1&param2=&param3=value3&param3")
        === ParsedInputPath("dir/file", emptySosOptions, Map("param1" -> "value1", "param2" -> "", "param3" -> null)))
    assert(parseInputPath("dir?sos-listing_strategy=*~42/@&foo=foo")
        === ParsedInputPath("dir", emptySosOptions.copy(listing_strategy = Some("*~42/@")), Map("foo" -> "foo")))
  }

  test("resolveInputs.fails") {
    val sparkIO = new SparkIO(sparkSession, emptySparkIOConfig)

    def im(path: String): String = {
      intercept[SToysException](sparkIO.resolveInputs(s"$tmpDir/$path")).getMessage
    }

    assert(im("foo.list?sos-table_name=bar").contains("not supported on *.list"))
    assert(im("foo?sos-listing_strategy=tables&sos-table_name=bar").contains("at the same time are not supported"))
    assert(im("foo?sos-listing_strategy=unsupported").contains("Unsupported listing strategy"))
    assert(im("foo?sos-listing_strategy=dag_unsupported").contains("Unsupported dag listing strategy"))
    assert(im(".foo").contains("Unsupported path starting with dot"))
  }

  test("resolveInputs") {
    writeTmpDagLists("aa", Seq.empty, Seq.empty, Seq("aa/aa"))
    writeTmpDagLists("a", Seq("aa"), Seq("aa/aa"), Seq("a/a"))
    writeTmpDagLists("b", Seq.empty, Seq("non_dag_table"), Seq("b/b"))
    writeTmpDagLists("dag", Seq("a", "b"), Seq("a/a", "b/b"), Seq("dag/foo", "dag/bar"))
    dfs.mkdirs(s"$tmpDir/dag/foo")
    dfs.mkdirs(s"$tmpDir/dag/bar")

    val dagAA = SosDag(s"$tmpDir/aa")
    val aa = emptySosTable.copy(table_name = "aa", path = s"$tmpDir/aa/aa")
    val dagA = SosDag(s"$tmpDir/a")
    val a = emptySosTable.copy(table_name = "a", path = s"$tmpDir/a/a")
    val dagB = SosDag(s"$tmpDir/b")
    val b = emptySosTable.copy(table_name = "b", path = s"$tmpDir/b/b")
    val nonDagTable = emptySosTable.copy(table_name = "non_dag_table", path = s"$tmpDir/non_dag_table")
    val dagDag = SosDag(s"$tmpDir/dag")
    val foo = emptySosTable.copy(table_name = "foo", path = s"$tmpDir/dag/foo")
    val bar = emptySosTable.copy(table_name = "bar", path = s"$tmpDir/dag/bar")

    val sparkIO = new SparkIO(sparkSession, emptySparkIOConfig)
    assert(sparkIO.resolveInputs(s"$tmpDir/dag/foo").toSet === Set(foo))
    assert(sparkIO.resolveInputs(s"$tmpDir/dag/foo?sos-table_name=renamed").toSet
        === Set(foo.copy(table_name = "renamed")))
    assert(sparkIO.resolveInputs(s"$tmpDir/dag/.dag").toSet === Set(dagDag, bar, foo))
    assert(sparkIO.resolveInputs(s"$tmpDir/dag/.dag/output_tables.list").toSet === Set(bar, foo))
    assert(sparkIO.resolveInputs(s"$tmpDir/dag?sos-listing_strategy=tables").toSet
        === Set(foo, bar).map(t => t.copy(path = s"file:${t.path}")))
    assert(sparkIO.resolveInputs(s"$tmpDir/dag?sos-listing_strategy=dag").toSet === Set(dagDag, bar, foo))
    assert(sparkIO.resolveInputs(s"$tmpDir/dag?sos-listing_strategy=dag_io").toSet === Set(dagDag, a, b, bar, foo))
    assert(sparkIO.resolveInputs(s"$tmpDir/dag?sos-listing_strategy=dag_io_recursive").toSet
        === Set(dagAA, aa, dagA, a, dagB, b, dagDag, bar, foo, nonDagTable))
  }

  test("addInputPaths") {
    writeTmpDagLists("dag", Seq.empty, Seq.empty, Seq("dag/foo"))
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

  test("Datasets.getAlias") {
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

  private def writeTmpDagLists(dagName: String,
      inputDags: Seq[String], inputTables: Seq[String], outputTables: Seq[String]): Unit = {
    dfs.writeString(s"$tmpDir/$dagName/.dag/input_dags.list",
      inputDags.map(d => s"$tmpDir/$d?sos-listing_strategy=dag").mkString("\n"))
    dfs.writeString(s"$tmpDir/$dagName/.dag/input_tables.list",
      inputTables.map(t => s"$tmpDir/$t").sorted.mkString("\n"))
    dfs.writeString(s"$tmpDir/$dagName/.dag/output_tables.list",
      outputTables.map(t => s"$tmpDir/$t").sorted.mkString("\n"))
  }
}

object SparkIOTest {
  case class Record(s: String)
}

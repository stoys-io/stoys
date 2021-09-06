package io.stoys.spark

import io.stoys.scala.Arbitrary
import io.stoys.spark.test.SparkTestBase

import java.nio.file.Path

class InputPathResolverTest extends SparkTestBase {
  import InputPathResolver._
  import InputPathResolverTest._

  private lazy val dfs = Dfs(sparkSession)

  test("parseInputPath") {
    val emptyStoysOptions = Arbitrary.empty[StoysOptions]
    assert(parseInputPath("dir") === ParsedInputPath("dir", emptyStoysOptions, Map.empty))
    assert(parseInputPath("dir/file") === ParsedInputPath("dir/file", emptyStoysOptions, Map.empty))
    assert(parseInputPath("?param=value") === ParsedInputPath("", emptyStoysOptions, Map("param" -> "value")))
    assert(parseInputPath("dir/file?param1=value1&param2=&param3=value3&param3")
        === ParsedInputPath("dir/file", emptyStoysOptions, Map("param1" -> "value1", "param2" -> "", "param3" -> null)))
    assert(parseInputPath("dir?sos-listing_strategy=*~42/@&foo=foo")
        === ParsedInputPath("dir", emptyStoysOptions.copy(listing_strategy = Some("*~42/@")), Map("foo" -> "foo")))
  }

  test("resolveInputs - fails") {
    val inputPathResolver = new InputPathResolver(dfs)

    def im(path: String): String = {
      intercept[SToysException](inputPathResolver.resolveInputs(s"$tmpDir/$path")).getMessage
    }

    assert(im("foo.list?sos-table_name=bar").contains("not supported on *.list"))
    assert(im("foo?sos-listing_strategy=tables&sos-table_name=bar").contains("at the same time are not supported"))
    assert(im("foo?sos-listing_strategy=unsupported").contains("Unsupported listing strategy"))
    assert(im("foo?sos-listing_strategy=dag_unsupported").contains("Unsupported dag listing strategy"))
    assert(im(".foo").contains("Unsupported path starting with dot"))
  }

  test("resolveInputs") {
    writeTmpDagLists(dfs, tmpDir, "aa", Seq.empty, Seq.empty, Seq("aa/aa"))
    writeTmpDagLists(dfs, tmpDir, "a", Seq("aa"), Seq("aa/aa"), Seq("a/a"))
    writeTmpDagLists(dfs, tmpDir, "b", Seq.empty, Seq("non_dag_table"), Seq("b/b"))
    writeTmpDagLists(dfs, tmpDir, "dag", Seq("a", "b"), Seq("a/a", "b/b"), Seq("dag/foo", "dag/bar"))
    dfs.mkdirs(s"$tmpDir/dag/foo")
    dfs.mkdirs(s"$tmpDir/dag/bar")

    val emptyStoysOptions = Arbitrary.empty[StoysOptions]
    val emptyTableInfo = Arbitrary.empty[TableInfo]

    val dagAA = DagInfo(s"$tmpDir/aa")
    val aa = emptyTableInfo.copy(tableName = "aa", path = s"$tmpDir/aa/aa")
    val dagA = DagInfo(s"$tmpDir/a")
    val a = emptyTableInfo.copy(tableName = "a", path = s"$tmpDir/a/a")
    val dagB = DagInfo(s"$tmpDir/b")
    val b = emptyTableInfo.copy(tableName = "b", path = s"$tmpDir/b/b")
    val nonDagTable = emptyTableInfo.copy(tableName = "non_dag_table", path = s"$tmpDir/non_dag_table")
    val dagDag = DagInfo(s"$tmpDir/dag")
    val foo = emptyTableInfo.copy(tableName = "foo", path = s"$tmpDir/dag/foo")
    val bar = emptyTableInfo.copy(tableName = "bar", path = s"$tmpDir/dag/bar")

    val resolver = new InputPathResolver(dfs)
    assert(resolver.resolveInputs(s"$tmpDir/dag/foo").toSet === Set(foo))
    assert(resolver.resolveInputs(s"$tmpDir/dag/foo?sos-table_name=renamed").toSet
        === Set(foo.copy(tableName = "renamed")))
    assert(resolver.resolveInputs(s"$tmpDir/dag/.dag").toSet === Set(dagDag, bar, foo))
    assert(resolver.resolveInputs(s"$tmpDir/dag/.dag/output_tables.list").toSet === Set(bar, foo))
    assert(resolver.resolveInputs(s"$tmpDir/dag?sos-listing_strategy=tables").toSet
        === Set(foo, bar).map(t => t.copy(path = s"file:${t.path}")))
    assert(resolver.resolveInputs(s"$tmpDir/dag?sos-listing_strategy=dag").toSet === Set(dagDag, bar, foo))
    assert(resolver.resolveInputs(s"$tmpDir/dag?sos-listing_strategy=dag_io").toSet === Set(dagDag, a, b, bar, foo))
    assert(resolver.resolveInputs(s"$tmpDir/dag?sos-listing_strategy=dag_io_recursive").toSet
        === Set(dagAA, aa, dagA, a, dagB, b, dagDag, bar, foo, nonDagTable))
  }
}

object InputPathResolverTest {
  def writeTmpDagLists(dfs: Dfs, tmpDir: Path, dagName: String,
      inputDags: Seq[String], inputTables: Seq[String], outputTables: Seq[String]): Unit = {
    dfs.writeString(s"$tmpDir/$dagName/.dag/input_dags.list",
      inputDags.map(d => s"$tmpDir/$d?sos-listing_strategy=dag").mkString("\n"))
    dfs.writeString(s"$tmpDir/$dagName/.dag/input_tables.list",
      inputTables.map(t => s"$tmpDir/$t").sorted.mkString("\n"))
    dfs.writeString(s"$tmpDir/$dagName/.dag/output_tables.list",
      outputTables.map(t => s"$tmpDir/$t").sorted.mkString("\n"))
  }
}

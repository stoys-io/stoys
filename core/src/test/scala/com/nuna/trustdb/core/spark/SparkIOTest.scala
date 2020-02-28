package com.nuna.trustdb.core.spark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import com.nuna.trustdb.core.SparkTestBase
import com.nuna.trustdb.core.util.{Arbitrary, IO}

class SparkIOTest extends SparkTestBase {
  import SparkIO._

  val dfs = new Dfs(sparkSession)

  val emptySparkIOConfig = Arbitrary.empty[SparkIOConfig]
  val emptySosTable = Arbitrary.empty[SosTable]

  test("parsePathParams") {
    assert(parsePathParams("dir") === ("dir", Seq.empty))
    assert(parsePathParams("dir/file") === ("dir/file", Seq.empty))
    assert(parsePathParams("?param=value") === ("", Seq("param" -> "value")))
    assert(parsePathParams("dir/file?param1=value1&param2=&param3=value3&param3")
        === ("dir/file", Seq("param1" -> "value1", "param2" -> "", "param3" -> "value3", "param3" -> null)))
    assert(parsePathParams("dir?sos-listing_strategy=*~42/@&foo=foo")
        === ("dir", Seq("sos-listing_strategy" -> "*~42/@", "foo" -> "foo")))
  }

  test("resolveInputTables") {
    val tmp = createTemporaryDirectory().toAbsolutePath
    val sparkIO = new SparkIO(sparkSession, emptySparkIOConfig)

    mkdirsWithSuccess(s"$tmp/foo")
    mkdirsWithSuccess(s"$tmp/bar")
    dfs.mkdirs(s"$tmp/baz")
    dfs.mkdirs(s"$tmp/.meta")
    writeStringToFile(s"$tmp/output.list", s"$tmp/foo\n$tmp/foo?sos-table_name=bar\n")

    assertThrows[IllegalArgumentException](sparkIO.resolveInputTables(s"$tmp/output.list?sos-table_name=bar"))
    assertThrows[IllegalArgumentException](
      sparkIO.resolveInputTables(s"$tmp/foo?sos-listing_strategy=tables&sos-table_name=bar"))

    assert(sparkIO.resolveInputTables(s"$tmp/output.list").toSet === Set(
      emptySosTable.copy(fullTableName = "bar", path = s"$tmp/foo"),
      emptySosTable.copy(fullTableName = "foo", path = s"$tmp/foo")))
    assert(sparkIO.resolveInputTables(s"$tmp/foo?sos-table_name=bar").toSet === Set(
      emptySosTable.copy(fullTableName = "bar", path = s"$tmp/foo")))
    assert(sparkIO.resolveInputTables(s"$tmp?sos-listing_strategy=dag").toSet === Set(
      emptySosTable.copy(fullTableName = "bar", path = s"$tmp/foo"),
      emptySosTable.copy(fullTableName = "foo", path = s"$tmp/foo")))
    assert(sparkIO.resolveInputTables(s"$tmp?sos-listing_strategy=tables").toSet === Set(
      emptySosTable.copy(fullTableName = "bar", path = s"file:$tmp/bar"),
      emptySosTable.copy(fullTableName = "baz", path = s"file:$tmp/baz"),
      emptySosTable.copy(fullTableName = "foo", path = s"file:$tmp/foo")))
  }

  def createTemporaryDirectory(deleteOnExit: Boolean = true): Path = {
    val directory = Files.createTempDirectory(this.getClass.getSimpleName + ".")
    if (deleteOnExit) {
      directory.toFile.deleteOnExit()
    }
    directory
  }

  def mkdirsWithSuccess(path: String): Unit = {
    dfs.mkdirs(path)
    dfs.createNewFile(s"$path/_SUCCESS")
  }

  def writeStringToFile(path: String, content: String): Unit = {
    IO.using(dfs.create(path))(_.write(content.getBytes(StandardCharsets.UTF_8)))
  }
}

package com.nuna.trustdb.core.spark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import com.nuna.trustdb.core.SparkTestBase
import com.nuna.trustdb.core.util.{Arbitrary, IO}

class SparkIOTest extends SparkTestBase {
  import SparkIO._

  val dfs = new Dfs(sparkSession)

  val emptySparkIOConfig = Arbitrary.empty[SparkIOConfig]
  val defaultSosParams = Arbitrary.empty[SosParams].copy(format = DEFAULT_FORMAT)

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

    mkdirsWithSuccess(s"$tmp/1/foo")
    mkdirsWithSuccess(s"$tmp/10/foo")
    mkdirsWithSuccess(s"$tmp/10/bar")
    mkdirsWithSuccess(s"$tmp/11/foo", success = false)
    mkdirsWithSuccess(s"$tmp/20/foo")
    writeStringToFile(s"$tmp/10.list", s"$tmp/10/foo\n$tmp/10/foo?sos-table_name=bar\n")

    assertThrows[IllegalArgumentException](sparkIO.resolveInputTables("foo.list?sos-table_name=bar"))
    assertThrows[IllegalArgumentException](sparkIO.resolveInputTables("foo?sos-listing_strategy=@&sos-table_name=bar"))

    assert(sparkIO.resolveInputTables(s"$tmp/10.list") === Seq(
      ReaderConfig("bar", s"$tmp/10/foo", defaultSosParams.format, Map.empty),
      ReaderConfig("foo", s"$tmp/10/foo", defaultSosParams.format, Map.empty)))

    assert(sparkIO.resolveInputTables(s"$tmp/10/foo?sos-table_name=bar") === Seq(
      ReaderConfig("bar", s"$tmp/10/foo", defaultSosParams.format, Map.empty)))

    assert(sparkIO.resolveInputTables(s"$tmp/10?sos-listing_strategy=@") === Seq(
      ReaderConfig("bar", s"file:$tmp/10/bar", defaultSosParams.format, Map.empty),
      ReaderConfig("foo", s"file:$tmp/10/foo", defaultSosParams.format, Map.empty)))
  }

  def createTemporaryDirectory(deleteOnExit: Boolean = true): Path = {
    val directory = Files.createTempDirectory(this.getClass.getSimpleName + ".")
    if (deleteOnExit) {
      directory.toFile.deleteOnExit()
    }
    directory
  }

  def mkdirsWithSuccess(path: String, success: Boolean = true): Unit = {
    dfs.mkdirs(path)
    if (success) {
      dfs.createNewFile(s"$path/_SUCCESS")
    }
  }

  def writeStringToFile(path: String, content: String): Unit = {
    IO.using(dfs.create(path))(_.write(content.getBytes(StandardCharsets.UTF_8)))
  }
}

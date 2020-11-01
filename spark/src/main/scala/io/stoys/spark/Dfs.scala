package io.stoys.spark

import java.nio.charset.StandardCharsets

import io.stoys.scala.IO
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession

// Philosophy:
//   1) asQualifiedPath is utility function to return concrete filesystem implementation and Path from path string.
//   2) There are a few utility functions like path, readString and writeString in here.
//   3) All other functions should have the same api as the underlying FileSystem api but taking path as string.
class Dfs(hadoopConfiguration: Configuration) {
  def path(path: String): Path = {
    new Path(path)
  }

  def asQualifiedPath(path: String): (FileSystem, Path) = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(hadoopConfiguration)
    val qualifiedPath = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    (fs, qualifiedPath)
  }

  def readString(path: String): String = {
    IO.using(open(path))(is => IOUtils.toString(is, StandardCharsets.UTF_8))
  }

  def writeString(path: String, content: String): Unit = {
    IO.using(create(path))(_.write(content.getBytes(StandardCharsets.UTF_8)))
  }

  def create(path: String): FSDataOutputStream = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.create(qualifiedPath)
  }

  def createNewFile(path: String): Boolean = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.createNewFile(qualifiedPath)
  }

  def exists(path: String): Boolean = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.exists(qualifiedPath)
  }

  def listStatus(path: String): Array[FileStatus] = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.listStatus(qualifiedPath)
  }

  def mkdirs(path: String): Boolean = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.mkdirs(qualifiedPath)
  }

  def open(path: String): FSDataInputStream = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.open(qualifiedPath)
  }
}

object Dfs {
  def apply(hadoopConfiguration: Configuration): Dfs = {
    new Dfs(hadoopConfiguration)
  }

  def apply(sparkSession: SparkSession): Dfs = {
    new Dfs(sparkSession.sparkContext.hadoopConfiguration)
  }
}

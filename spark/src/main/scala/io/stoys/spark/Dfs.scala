package io.stoys.spark

import java.nio.charset.StandardCharsets

import io.stoys.scala.IO
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession

/**
 * [[Dfs]] is utility wrapper around hadoop [[FileSystem]] library. There are following types of functions:
 * 1) [[asQualifiedPath]] returns concrete [[FileSystem]] implementation and [[Path]] from [[String]] path
 * 2) Utility functions like [[path]], [[fs]], [[readString]] and [[writeString]]
 * 3) All other functions should have the same api as the underlying FileSystem api but taking path as string
 *
 * @param hadoopConfiguration hadoop configuration (can come from [[SparkSession]] using [[Dfs]].apply)
 */
class Dfs(hadoopConfiguration: Configuration) {
  def path(path: String): Path = {
    new Path(path)
  }

  def fs(path: String): FileSystem = {
    fs(new Path(path))
  }

  def fs(path: Path): FileSystem = {
    path.getFileSystem(hadoopConfiguration)
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

  // Proxy methods from String path to hdfs library...

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

  def getContentSummary(path: String): ContentSummary = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.getContentSummary(qualifiedPath)
  }

  def getFileChecksum(path: String): FileChecksum = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.getFileChecksum(qualifiedPath)
  }

  def getFileStatus(path: String): FileStatus = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.getFileStatus(qualifiedPath)
  }

  def globStatus(path: String): Array[FileStatus] = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.globStatus(qualifiedPath)
  }

  def isDirectory(path: String): Boolean = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.isDirectory(qualifiedPath)
  }

  def isFile(path: String): Boolean = {
    val (fs, qualifiedPath) = asQualifiedPath(path)
    fs.isFile(qualifiedPath)
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

package io.stoys.spark.test

import java.net.URL
import java.nio.file.{Path, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.runtime.universe._

class DataCache(sparkSession: SparkSession, dataDir: String = "data_cache", cacheDir: String = "target/data_cache") {
  import sparkSession.implicits._

  private val dataPath = Paths.get(dataDir)
  private val cachePath = Paths.get(cacheDir)

  def getLocallyCachedFilePath(url: String): Path = {
//    val localPath = dataPath.resolve(StolenUtils.toWordCharactersCollapsing(url))
    val localPath = dataPath.resolve(Paths.get(new URL(url).getPath).getFileName.toString)
    if (!localPath.toFile.exists()) {
      println(s"Downloading $url to $localPath (this may take a while) ...")
      FileUtils.copyURLToFile(new URL(url), localPath.toFile)
    }
    localPath
  }

  def readLocallyCachedFileDf(fileUrl: String, format: String, options: Map[String, String]): DataFrame = {
    val path = getLocallyCachedFilePath(fileUrl)
    readOrCreateCachedDf(sparkSession, path.getFileName.toString) {
      sparkSession.read.format(format).options(options).load(path.toString)
    }
  }

  def readLocallyCachedCsvFileDf(csvFileUrl: String): DataFrame = {
    readLocallyCachedFileDf(csvFileUrl, "csv", Map("header" -> "true"))
  }

  def readOrCreateCachedDf(sparkSession: SparkSession, cacheKey: String)(computeDf: => DataFrame): DataFrame = {
    val cachedPath = cachePath.resolve(StolenUtils.toWordCharactersCollapsing(cacheKey))
    if (!cachedPath.toFile.exists()) {
      println(s"Caching the data frame in $cachedPath ...")
      val df = computeDf
      df.write.format("parquet").mode("overwrite").save(cachedPath.toString)
      println(s"Cached in $cachedPath")
    }
    sparkSession.read.format("parquet").load(cachedPath.toString)
  }

  def readCachedDs[T <: Product : TypeTag](cacheKey: String)(computeDs: => Dataset[T]): Dataset[T] = {
    readOrCreateCachedDf(sparkSession, cacheKey)(computeDs.toDF()).as[T]
  }
}

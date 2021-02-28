package io.stoys.spark.test

import java.net.URL
import java.nio.file.{Path, Paths}

import io.stoys.scala.Strings
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class DataCache(sparkSession: SparkSession, dataDir: String = "data_cache", cacheDir: String = "target/data_cache") {
  private val logger = org.log4s.getLogger

  private val dataPath = Paths.get(dataDir)
  private val cachePath = Paths.get(cacheDir)

  def getLocallyCachedFilePath(url: String): Path = {
//    val fileName = Strings.toWordCharactersCollapsing(url)
    val fileName = Paths.get(new URL(url).getPath).getFileName.toString
    val localPath = dataPath.resolve(fileName)
    if (!localPath.toFile.exists()) {
      logger.info(s"Downloading $url to $localPath (this may take a while) ...")
      FileUtils.copyURLToFile(new URL(url), localPath.toFile)
      logger.info("Downloading finished.")
    }
    localPath
  }

  def readLocallyCachedFileDf(fileUrl: String, format: String, options: Map[String, String]): DataFrame = {
    val path = getLocallyCachedFilePath(fileUrl)
    val cacheKey = path.getFileName.toString
    readOrCreateCachedDf(cacheKey, sparkSession.read.format(format).options(options).load(path.toString))
  }

  def readOrCreateCachedDf[T](cacheKey: String, computeDf: => Dataset[T]): DataFrame = {
    val cachedPath = cachePath.resolve(Strings.toWordCharactersCollapsing(cacheKey))
    if (!cachedPath.toFile.exists()) {
      logger.info(s"Caching the data frame in $cachedPath ...")
      val df = computeDf
      df.write.format("parquet").mode("overwrite").save(cachedPath.toString)
      logger.info("Caching finished.")
    }
    sparkSession.read.format("parquet").load(cachedPath.toString)
  }
}

package io.stoys.spark.test.datasets

import io.stoys.spark.test.DataCache
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Date

// https://github.com/GoogleCloudPlatform/covid-19-open-data
class Covid19Dataset(sparkSession: SparkSession) {
  private val dataCache = new DataCache(sparkSession)

  def readCachedCovid19Csv(fileName: String): DataFrame = {
    val url = s"https://storage.googleapis.com/covid19-open-data/v2/$fileName"
    dataCache.readLocallyCachedFileDf(url, "csv", Map("header" -> "true"))
  }
}

object Covid19Dataset {
  case class Epidemiology(
      key: String,
      date: Date,
      new_confirmed: Int,
      new_deceased: Int,
      new_recovered: Int,
      new_tested: Int,
      total_confirmed: Int,
      total_deceased: Int,
      total_recovered: Int,
      total_tested: Int,
  )

  case class Demographics(
      key: String,
      population: Int,
  )
}

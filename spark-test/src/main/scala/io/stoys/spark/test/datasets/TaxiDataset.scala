package io.stoys.spark.test.datasets

import io.stoys.spark.test.DataCache
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

// https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
class TaxiDataset(sparkSession: SparkSession) {
  private val dataCache = new DataCache(sparkSession)

  private def computeTripDataPlus(tripDataDf: DataFrame): DataFrame = {
    val zoo = "🐧 🐵 🐈 🐕 🐢 🐙 🐌 🐏".split(' ').toSeq
//    val zoo = "🐧 🐵 🐈 🐕 🐢 🐬 🐦 🐙 🐌 🐝 🐸 🕷 🐏 🐍 🐫 🐳".split(' ').toSeq
    val animalIndexExpr = (c: Column) => pmod(hash(col("*"), c), lit(zoo.size)).cast(IntegerType) + 1
    val animalExpr = (c: Column) => element_at(array(zoo.map(lit): _*), animalIndexExpr(c))
    val indicesExpr = sequence(typedLit(1), col("passenger_count").cast(IntegerType))
    val passengersListExpr = transform(indicesExpr, animalExpr)
    val passengersExpr = array_join(passengersListExpr, "")
    tripDataDf.withColumn("passengers", passengersExpr)
//        .withColumn("passengers_list", passengersListExpr)
//        .withColumn("cats", lit(" ~=[,,_,,]:3 " * 42))
  }

  def readCachedYellowTripDataCsv(fileName: String): DataFrame = {
    val url = s"https://s3.amazonaws.com/nyc-tlc/trip+data/$fileName"
    dataCache.readLocallyCachedFileDf(url, "csv", Map("header" -> "true"))
  }

  def readCachedYellowTripDataPlusCsv(fileName: String): DataFrame = {
    computeTripDataPlus(readCachedYellowTripDataCsv(fileName))
//    dataCache.readOrCreateCachedDf("tripDataPlusDf", computeTripDataPlus(readCachedYellowTripDataCsv(fileName)))
  }
}

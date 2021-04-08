package io.stoys.spark.test.datasets

import io.stoys.spark.test.DataCache
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.sql.Timestamp

// https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
class TaxiDataset(sparkSession: SparkSession) {
  private val dataCache = new DataCache(sparkSession)

  private def computeTripDataPlus(tripDataDf: DataFrame): DataFrame = {
    val zoo = "🐧 🐵 🐈 🐕 🐢 🐙 🐌 🐏".split(' ').toSeq
//    val zoo = "🐧 🐵 🐈 🐕 🐢 🐬 🐦 🐙 🐌 🐝 🐸 🕷 🐏 🐍 🐫 🐳".split(' ').toSeq
    val animalIndexExpr = (c: Column) => pmod(hash(col("*"), c), lit(zoo.size)).cast(IntegerType) + 1
    val animalExpr = (c: Column) => element_at(array(zoo.map(lit): _*), animalIndexExpr(c))
    val passengersExpr = transform(sequence(typedLit(1), col("passenger_count").cast(IntegerType)), animalExpr)
    tripDataDf.withColumn("passengers", passengersExpr).withColumn("cats", lit(" ~=[,,_,,]:3 " * 42))
  }

  def readCachedYellowTripDataCsv(fileName: String): DataFrame = {
    val url = s"https://s3.amazonaws.com/nyc-tlc/trip+data/$fileName"
    dataCache.readLocallyCachedFileDf(url, "csv", Map("header" -> "true"))
  }

  def readCachedYellowTripDataPlusCsv(fileName: String): DataFrame = {
    computeTripDataPlus(readCachedYellowTripDataCsv(fileName))
//    dataCache.readOrCreateCachedDf("tripDataPlusDf", computeTripDataPlus(readCachedYellowTripDataCsv(fileName)))
  }

  // TODO: Remove this transform after dropping Spark 2.4.x support.
  private def transform(column: Column, f: Column => Column): Column = {
    import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, LambdaFunction, UnresolvedNamedLambdaVariable}
    def createLambda(f: Column => Column) = {
      val x = UnresolvedNamedLambdaVariable(Seq("x"))
      val function = f(new Column(x)).expr
      LambdaFunction(function, Seq(x))
    }
    new Column(ArrayTransform(column.expr, createLambda(f)))
  }
}

package io.stoys.spark.excel

import java.sql.Date

import io.stoys.scala.IO
import io.stoys.spark.test.{DataCache, SparkTestBase}
import io.stoys.spark.{Reshape, ReshapeConfig}
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe.TypeTag

@org.scalatest.DoNotDiscover
class Covid19EpidemiologyExample extends SparkTestBase {
  import Covid19EpidemiologyExample._

  private lazy val dataCache = new DataCache(sparkSession)

  test("covid19_epidemiology") {
    readCachedCovid19Csv[Epidemiology]("epidemiology.csv").createOrReplaceTempView("epidemiology")
    readCachedCovid19Csv[Demographics]("demographics.csv").createOrReplaceTempView("demographics")

    val elyDf = sparkSession.sql(IO.resourceToString(this.getClass, "epidemiology_last_year.sql"))
    val etrDf = sparkSession.sql(IO.resourceToString(this.getClass, "epidemiology_top_regions.sql"))

    val epidemiologyTemplateXlsx = IO.resourceToByteArray(this.getClass, "epidemiology.template.xlsx")
    val config = ExcelWriterConfig.default.copy(template_xlsx = epidemiologyTemplateXlsx)
    val excelFilesPerRowDf = SparkExcelWriter.datasetsToExcelFilesPerRow(Seq(elyDf, etrDf), config)

    excelFilesPerRowDf.cache()
//    writeTmpDs("tmp_xlsx", excelFilesPerRowDf, "file_per_row")
//    writeTmpDs("tmp_zip", excelFilesPerRowDf.coalesce(1), "zip",
//      Map("zip_method" -> "STORED", "zip_file_name" -> "epidemiology.zip"))
    excelFilesPerRowDf.write.format("file_per_row").save(s"$tmpDir/covid19_epidemiology/xlsx")
    excelFilesPerRowDf.coalesce(1).write.format("zip")
        .option("zip_method", "STORED").option("zip_file_name", "epidemiology.zip")
        .save(s"$tmpDir/covid19_epidemiology/zip")
    excelFilesPerRowDf.unpersist()
  }

  def readCachedCovid19Csv[T <: Product : TypeTag](fileName: String): Dataset[T] = {
    // https://github.com/GoogleCloudPlatform/covid-19-open-data
    val url = s"https://storage.googleapis.com/covid19-open-data/v2/$fileName"
    val df = dataCache.readLocallyCachedFileDf(url, "csv", Map("header" -> "true"))
    Reshape.reshape[T](df, ReshapeConfig.dangerous)
  }
}

object Covid19EpidemiologyExample {
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
      total_tested: Int
  )

  case class Demographics(
      key: String,
      population: Int
  )
}

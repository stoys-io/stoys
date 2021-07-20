package io.stoys.spark.excel

import io.stoys.scala.{IO, Reflection}
import io.stoys.spark.test.SparkExampleBase
import io.stoys.spark.test.datasets.Covid19Dataset
import io.stoys.spark.{Reshape, ReshapeConfig}
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe.TypeTag

class Covid19ExcelExample extends SparkExampleBase {
  private val logger = org.log4s.getLogger

  private lazy val covid19Dataset = new Covid19Dataset(sparkSession)

  test("covid19_excel") {
    readAndRegisterCachedCovid19Csv[Covid19Dataset.Epidemiology]
    readAndRegisterCachedCovid19Csv[Covid19Dataset.Demographics]

    val elyDf = sparkSession.sql(IO.resourceToString(this.getClass, "epidemiology_last_year.sql"))
    val etrDf = sparkSession.sql(IO.resourceToString(this.getClass, "epidemiology_top_regions.sql"))

    val epidemiologyTemplateXlsx = IO.resourceToByteArray(this.getClass, "epidemiology.template.xlsx")
    val config = ExcelWriterConfig.default.copy(template_xlsx = epidemiologyTemplateXlsx)
    val excelFilesPerRowDf = SparkExcelWriter.datasetsToExcelFilesPerRow(Seq(elyDf, etrDf), config)

    excelFilesPerRowDf.cache()
//    writeTmpDs("tmp_xlsx", excelFilesPerRowDf, "file_per_row")
//    writeTmpDs("tmp_zip", excelFilesPerRowDf.coalesce(1), "zip",
//      Map("zip_method" -> "STORED", "zip_file_name" -> "epidemiology.zip"))
    excelFilesPerRowDf.write.format("file_per_row").save(s"$tmpDir/xlsx")
    excelFilesPerRowDf.coalesce(1).write.format("zip")
        .option("zip_method", "STORED").option("zip_file_name", "epidemiology.zip").save(s"$tmpDir/zip")
    excelFilesPerRowDf.unpersist()

    val epidemiologyUsXlsxPath = tmpDir.resolve("xlsx/epidemiology_US.xlsx")
    logger.info(s"File epidemiology_US.xlsx written to:\n${highlight(epidemiologyUsXlsxPath.toAbsolutePath)}")
    assert(epidemiologyUsXlsxPath.toFile.exists())
    val epidemiologyZipPath = tmpDir.resolve("zip/epidemiology.zip")
    logger.info(s"File epidemiology.zip written to:\n${highlight(epidemiologyZipPath.toAbsolutePath)}")
    assert(epidemiologyZipPath.toFile.exists())
  }

  private def readAndRegisterCachedCovid19Csv[T <: Product : TypeTag]: Dataset[T] = {
    val tableName = Reflection.typeNameOf[T].toLowerCase
    val fileName = s"$tableName.csv"
    val df = covid19Dataset.readCachedCovid19Csv(fileName)
    val ds = Reshape.reshape[T](df, ReshapeConfig.dangerous)
    ds.createOrReplaceTempView(tableName)
    ds
  }
}

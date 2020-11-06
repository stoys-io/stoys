package io.stoys.spark.excel

import java.sql.Date

import io.stoys.scala.IO
import io.stoys.spark.datasources.BinaryFilePerRow
import io.stoys.spark.test.SparkTestBase
import org.apache.spark.SparkException
import org.apache.spark.sql.functions._

class SparkExcelWriterTest extends SparkTestBase {
  import ExcelWriterTest._
  import SparkExcelWriterTest._
  import io.stoys.spark.test.implicits._
  import sparkSession.implicits._

  val orders = Seq(
    Order(1, "customer_1", "item_1", 1, "2020-02-20", 100),
    Order(2, "customer_1", "item_2", 1, "2020-02-20", 700),
    Order(3, "customer X", "item_1", 3, "2020-02-20", 300),
    Order(4, "customer_2", "item_3", 1, "2020-01-10", 42),
    Order(5, "customer_1", "item_1", 3, "2020-01-10", 300),
    Order(6, "customer_1", "item_2", 6, "2020-02-20", 4200)
  )

  test("datasetToExcelFilesPerRow") {
    val excelFilesPerRowDf = SparkExcelWriter.datasetToExcelFilesPerRow(orders.toDF())
    excelFilesPerRowDf.cache()
    excelFilesPerRowDf.write.format("file_per_row").save(s"$tmpDir/datasetToExcelFilesPerRow")
    val excelFilesPerRow = excelFilesPerRowDf.as[BinaryFilePerRow].collect()
    excelFilesPerRowDf.unpersist()

    assert(excelFilesPerRow.length === 1)
    assert(excelFilesPerRow.map(_.path).contains("workbook.xlsx"))
    val workbook = deserializeWorkbook(excelFilesPerRow.head.content)
    assert(workbook.getNumberOfSheets === 1)
    val sheet = workbook.getSheet("Sheet1")
    assert(sheet !== null)
    val cellsByAddress = getCellsByAddress(sheet)
    assert(cellsByAddress.get("F2").map(_.getNumericCellValue) === Some(1.0))
  }

  test("datasetsToExcelFilesPerRow.zeroDatasetsUnsupported") {
    val intercepted = intercept[SparkException](SparkExcelWriter.datasetsToExcelFilesPerRow(Seq.empty))
    assert(intercepted.getMessage.contains("At least one dataset required"))
  }

  test("datasetsToExcelFilesPerRow") {
    orders.toDS().createOrReplaceTempView("order")

    val allOrdersDf = orders.toDF().withColumn("__sheet__", lit("Orders"))
    val ordersByCustomerDf = sparkSession.sql(
      s"""
         |SELECT
         |  CONCAT(customer, "/", "orders_", REGEXP_REPLACE(customer, "\\\\W+", "_"), ".xlsx") AS __path__,
         |  CONCAT("Orders By ", customer) AS __sheet__,
         |  *
         |FROM order
         |""".stripMargin.trim)
    val spendByCustomerDf = sparkSession.sql(
      s"""
         |SELECT
         |  CONCAT(customer, "/", "orders_", REGEXP_REPLACE(customer, "\\\\W+", "_"), ".xlsx") AS __path__,
         |  "Spend Per Item By This Customer" AS __sheet__,
         |  customer,
         |  item,
         |  SUM(cost_cents) AS cost_cents
         |FROM order
         |GROUP BY customer, item
         |ORDER BY customer, item
         |""".stripMargin.trim)
    val dfs = Seq(allOrdersDf, ordersByCustomerDf, spendByCustomerDf)
    val excelFilesPerRowDf = SparkExcelWriter.datasetsToExcelFilesPerRow(dfs, ordersConfig)
    excelFilesPerRowDf.cache()
    excelFilesPerRowDf.write.format("file_per_row").save(s"$tmpDir/datasetsToExcelFilesPerRow")
    excelFilesPerRowDf.coalesce(1).write.format("zip")
        .option("zip_method", "STORED").option("zip_file_name", "orders.zip")
        .save(s"$tmpDir/datasetsToExcelFilesPerRow.zip")
    val excelFilesPerRow = excelFilesPerRowDf.as[BinaryFilePerRow].collect()
    excelFilesPerRowDf.unpersist()

    assert(excelFilesPerRow.length === 3)
    assert(excelFilesPerRow.map(_.path).contains("customer X/orders_customer_X.xlsx"))
  }

  test("to_excel") {
    orders.toDS().createOrReplaceTempView("order")
    sparkSession.udf.register("to_excel", SparkExcelWriter.toExcel _)

    sparkSession.sql(
      s"""
         |INSERT OVERWRITE DIRECTORY '$tmpDir/to_excel' USING file_per_row
         |SELECT
         |  "orders.xlsx" AS path,
         |  to_excel(NAMED_STRUCT("orders", COLLECT_LIST(STRUCT(*))), NULL) AS content
         |FROM order
         |GROUP BY true
         |""".stripMargin.trim)

    val outputFileStatuses = walkDfsFileStatusesByRelativePath(s"$tmpDir/to_excel")
    assert(outputFileStatuses.keySet === Set("orders.xlsx"))
  }

  test("to_excel.complex") {
    orders.toDS().createOrReplaceTempView("order")
    sparkSession.udf.register("to_excel", SparkExcelWriter.toExcel _)

    val ordersTemplateXlsxDs = Seq(BinaryFilePerRow("orders.template.xlsx", ordersTemplateXlsx)).toDS()
    ordersTemplateXlsxDs.createOrReplaceTempView("orders_template_xlsx")

    val excelFilesPerRowDf = sparkSession.sql(IO.resourceToString(this.getClass, "orders_to_excel_complex.sql"))
    excelFilesPerRowDf.cache()
    excelFilesPerRowDf.write.format("file_per_row").save(s"$tmpDir/to_excel.complex")
    val excelFilesPerRow = excelFilesPerRowDf.as[BinaryFilePerRow].collect()
    excelFilesPerRowDf.unpersist()

    assert(excelFilesPerRow.length === 3)
    assert(excelFilesPerRow.map(_.path).contains("customer X/orders_customer_X.xlsx"))
  }
}

object SparkExcelWriterTest {
  case class Order(id: Int, customer: Option[String], item: String, amount: Int, date: Date, cost_cents: Long)
}

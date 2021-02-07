package io.stoys.spark.excel

import java.io.ByteArrayInputStream
import java.nio.file.Files

import io.stoys.scala.IO
import io.stoys.spark.test.SparkTestBase
import org.apache.poi.ss.usermodel.{Cell, CellType, Sheet, Workbook}
import org.apache.poi.ss.util.CellAddress
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import scala.jdk.CollectionConverters._

class ExcelWriterTest extends SparkTestBase {
  import ExcelWriter._
  import ExcelWriterTest._

  private def readTemplateWorkbook(fileName: String): Workbook = {
    IO.using(this.getClass.getResourceAsStream(fileName))(is => new XSSFWorkbook(is))
  }

  private def writeDebugWorkbook(workbook: Workbook, testName: String): Unit = {
    IO.using(Files.newOutputStream(tmpDir.resolve(s"$testName.xlsx")))(workbook.write)
  }

  test("createWorkbook") {
    val defaultWorkbook = createWorkbook(defaultConfig)
    assert(getSpecialCellNames(defaultWorkbook).size === 0)
    assert(!defaultWorkbook.isInstanceOf[SXSSFWorkbook])
    val streamingWorkbook = createWorkbook(defaultConfig.copy(poi_streaming_row_access_window_size = Some(42)))
    assert(streamingWorkbook.isInstanceOf[SXSSFWorkbook])
    val templateXlsx = IO.resourceToByteArray(this.getClass, ordersTemplateFileName)
    assert(getSpecialCellNames(createWorkbook(defaultConfig.copy(template_xlsx = templateXlsx))).size === 4)
  }

  test("extractSheetConfig") {
    val workbook = readTemplateWorkbook(ordersTemplateFileName)
    assert(workbook.getNumberOfSheets === 1)
    assert(getStringCells(workbook).size === 5)
    assert(getSpecialCellNames(workbook).size === 4)
    val sheetConfig = extractSheetConfig(workbook.getSheet("Orders"))
    writeDebugWorkbook(workbook, "extractSheetConfig")
    assert(sheetConfig.sheetName === "Orders")
    assert(sheetConfig.startAddress === Some(new CellAddress("B4")))
    assert(getStringCells(workbook).size === 1)
    assert(getSpecialCellNames(workbook).isEmpty)
  }

  test("writeTableDataIntoWorkbook") {
    val valueConverter = (v: Any) => v.asInstanceOf[java.lang.Long].toFloat / 100.0
    val columnInfo = Seq(
      ColumnInfo("__rowid__", None),
      ColumnInfo("value", None, converter = Some(valueConverter)),
      ColumnInfo("__extra_system_columns_are_ignored__", None)
    )
    val tableData = Seq(
      Seq[Any](2L, 4200L, "foo"),
      Seq[Any](1L, 42L, "bar")
    )

    val workbook = readTemplateWorkbook(ordersTemplateFileName)
    writeTableDataIntoWorkbook(workbook, "Orders", columnInfo, tableData, ExcelWriterConfig.default)
    writeDebugWorkbook(workbook, "writeTableDataIntoWorkbook")

    val sheet = workbook.getSheet("Orders")
    val cellsByAddress = getCellsByAddress(sheet)
    assert(cellsByAddress.get("A4").map(_.getStringCellValue) === None)
    assert(cellsByAddress.get("B4").map(_.getStringCellValue) === Some("value"))
    assert(cellsByAddress.get("C4").map(_.getStringCellValue) === None)
    assert(cellsByAddress.get("B5").map(_.getNumericCellValue) === Some(0.42))
    assert(cellsByAddress.get("B6").map(_.getNumericCellValue) === Some(42.0))
    assert(cellsByAddress.get("B7") === None)
    assert(getSpecialCellNames(workbook).isEmpty)
    // TODO: test auto sizing, conditional formatting, data format string, ...
  }
}

object ExcelWriterTest {
  val defaultConfig: ExcelWriterConfig = ExcelWriterConfig.default
  val ordersTemplateFileName: String = "orders.template.xlsx"
  val ordersTemplateXlsx: Array[Byte] = IO.resourceToByteArray(this.getClass, ordersTemplateFileName)
  val ordersConfig: ExcelWriterConfig = ExcelWriterConfig.default.copy(template_xlsx = ordersTemplateXlsx)

  def deserializeWorkbook(serialized: Array[Byte]): Workbook = {
    IO.using(new ByteArrayInputStream(serialized))(is => new XSSFWorkbook(is))
  }

  def getCellsByAddress(sheet: Sheet): Map[String, Cell] = {
    val cells = sheet.rowIterator().asScala.flatMap(_.cellIterator().asScala)
    cells.map(c => c.getAddress.toString -> c).toMap
  }

  def getStringCells(workbook: Workbook): Seq[Cell] = {
    val cells = workbook.sheetIterator().asScala.flatMap(_.rowIterator().asScala.flatMap(_.cellIterator().asScala))
    cells.filter(_.getCellType == CellType.STRING).toSeq
  }

  def getSpecialCellNames(workbook: Workbook): Seq[String] = {
    getStringCells(workbook).map(_.getStringCellValue).filter(_.matches("<<.*>>"))
  }
}

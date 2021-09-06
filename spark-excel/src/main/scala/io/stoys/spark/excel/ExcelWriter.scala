package io.stoys.spark.excel

import io.stoys.scala.IO
import io.stoys.spark.SToysException
import org.apache.poi.ss.usermodel._
import org.apache.poi.ss.util.{CellAddress, CellRangeAddress}
import org.apache.poi.xssf.streaming.{SXSSFSheet, SXSSFWorkbook}
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.jdk.CollectionConverters._
import scala.util.Try

object ExcelWriter {
  private[excel] val CONFIG_CELL_PATTERN = "^<<([^:]*)(?::(.*))?>>$".r("key", "params")
  private[excel] val SPECIAL_COLUMN_NAME_PATTERN = "^__(.*)__$".r("key")
  private val UNITS_PER_CHARACTER_WIDTH = 256

  private object ConfigCellType extends Enumeration {
    type ConfigCellType = Value
    val UNDEFINED, TABLE_STARTS_HERE, HEADER_STYLE, CELL_STYLE, COLUMN_STYLE = Value
  }

  private[excel] case class ComplexStyle(
      cellStyle: CellStyle,
      conditionalFormatting: Option[ConditionalFormatting]
  )

  private[excel] case class SheetConfig(
      sheetName: String,
      startAddress: Option[CellAddress],
      headerStyle: Option[ComplexStyle],
      cellStyle: Option[ComplexStyle],
      columnStyles: Map[String, ComplexStyle]
  )

  private[excel] object SheetConfig {
    def fromSheetName(sheetName: String): SheetConfig = {
      SheetConfig(sheetName, None, None, None, Map.empty)
    }
  }

  private[excel] case class ColumnInfo(
      columnName: String,
      dataFormatString: Option[String],
      converter: Option[Any => Any] = None
  )

  def createWorkbook(config: ExcelWriterConfig): Workbook = {
    val workbook = config.template_xlsx match {
      case null => new XSSFWorkbook()
      case template => IO.using(new ByteArrayInputStream(template))(bais => new XSSFWorkbook(bais))
    }

    config.poi_streaming_row_access_window_size.map(size => new SXSSFWorkbook(workbook, size)).getOrElse(workbook)
  }

  private[excel] def extractSheetConfig(sheet: Sheet): SheetConfig = {
    val conditionalFormattingByCellAddress = extractConditionalFormattingByCellAddress(sheet)
    val cellIterator = sheet.rowIterator().asScala.flatMap(_.cellIterator().asScala)
    val stringCells = cellIterator.filter(_.getCellType == CellType.STRING)
    var sheetConfig = SheetConfig.fromSheetName(sheet.getSheetName)
    stringCells.toArray.foreach { cell =>
      cell.getStringCellValue.toUpperCase match {
        case CONFIG_CELL_PATTERN(key, params) =>
          val cellAddress = cell.getAddress
          val conditionalFormatting = conditionalFormattingByCellAddress.get(cellAddress)
          conditionalFormatting.foreach(cf => cf.setFormattingRanges(cf.getFormattingRanges.filterNot {
            fr => fr.getFirstRow == cellAddress.getRow && fr.getFirstColumn == cellAddress.getColumn
          }))
          val complexStyle = ComplexStyle(cell.getCellStyle, conditionalFormatting)
          Try(ConfigCellType.withName(key)).toOption.getOrElse(ConfigCellType.UNDEFINED) match {
            case ConfigCellType.TABLE_STARTS_HERE =>
              sheetConfig = sheetConfig.copy(startAddress = Some(cellAddress))
              cell.setBlank()
            case ConfigCellType.HEADER_STYLE =>
              sheetConfig = sheetConfig.copy(headerStyle = Some(complexStyle))
              cell.getRow.removeCell(cell)
            case ConfigCellType.CELL_STYLE =>
              sheetConfig = sheetConfig.copy(cellStyle = Some(complexStyle))
              cell.getRow.removeCell(cell)
            case ConfigCellType.COLUMN_STYLE =>
              sheetConfig = sheetConfig.copy(columnStyles = sheetConfig.columnStyles.updated(params, complexStyle))
              cell.getRow.removeCell(cell)
            case _ => throw new SToysException(s"Unsupported configuration key '$key' on sheet ${sheet.getSheetName}.")
          }
        case _ =>
      }
    }
    sheetConfig
  }

  private def extractConditionalFormattingByCellAddress(sheet: Sheet): Map[CellAddress, ConditionalFormatting] = {
    val sheetConditionalFormatting = sheet.getSheetConditionalFormatting
    val sheetConditionalFormattingCount = sheetConditionalFormatting.getNumConditionalFormattings
    val conditionalFormattingOfSingleCells = 0.until(sheetConditionalFormattingCount).flatMap { i =>
      val conditionalFormatting = sheetConditionalFormatting.getConditionalFormattingAt(i)
      conditionalFormatting.getFormattingRanges match {
        case Array(cra) if cra.getNumberOfCells == 1 =>
          Some(new CellAddress(cra.getFirstRow, cra.getFirstColumn) -> conditionalFormatting)
        case _ => None
      }
    }
    conditionalFormattingOfSingleCells.toMap
  }

  def writeTableDataIntoWorkbook(workbook: Workbook, sheetName: String, columnInfo: Seq[ColumnInfo],
      tableData: Seq[Seq[Any]], config: ExcelWriterConfig): Unit = {
    val (processedColumnInfo, processedTableData) = processSpecialColumns(columnInfo, tableData)
    val convertedTableData = convertTableData(processedColumnInfo, processedTableData)

    val sheet = Option(workbook.getSheet(sheetName)).getOrElse(workbook.createSheet(sheetName))
    val sheetConfig = extractSheetConfig(sheet)
    val startAddress = sheetConfig.startAddress.getOrElse(new CellAddress("A1"))

    if (config.use_header) {
      val columnNames = Seq(processedColumnInfo.map(_.columnName))
      val headerColumnStyles = Seq.fill(processedColumnInfo.size)(sheetConfig.headerStyle.orNull)
      writeTableDataIntoSheet(sheet, startAddress, columnNames, headerColumnStyles)
    }

    if (config.auto_size_columns && config.poi_based_auto_size_columns && sheet.isInstanceOf[SXSSFSheet]) {
      sheet.asInstanceOf[SXSSFSheet].trackAllColumnsForAutoSizing()
    }

    val headerRowsCount = if (config.use_header) 1 else 0
    val dataStartAddress = new CellAddress(startAddress.getRow + headerRowsCount, startAddress.getColumn)
    val columnStyles = processedColumnInfo.map { ci =>
      val columnName = ci.columnName.toUpperCase
      val columnStyle = sheetConfig.columnStyles.get(columnName).orElse(sheetConfig.cellStyle)
      val cellStyle = workbook.createCellStyle()
      columnStyle.foreach(cs => cellStyle.cloneStyleFrom(cs.cellStyle))
      val dataFormat = workbook.createDataFormat()
      ci.dataFormatString.foreach(dfs => cellStyle.setDataFormat(dataFormat.getFormat(dfs)))
      columnStyle.map(_.copy(cellStyle = cellStyle)).getOrElse(ComplexStyle(cellStyle, None))
    }
    writeTableDataIntoSheet(sheet, dataStartAddress, convertedTableData, columnStyles)

    if (config.auto_size_columns) {
      if (config.poi_based_auto_size_columns) {
        processedColumnInfo.indices.foreach(i => sheet.autoSizeColumn(startAddress.getColumn + i))
      } else {
        val autoSizeConfig = config.auto_size_config
        autoSizeColumnsByMaxStringWidth(sheet, startAddress, processedColumnInfo, convertedTableData, autoSizeConfig)
      }
    }
  }

  private def writeTableDataIntoSheet(sheet: Sheet, startCellAddress: CellAddress, tableData: Seq[Seq[Any]],
      columnStyles: Seq[ComplexStyle]): Unit = {
    tableData.indices.foreach { rowIndex =>
      val rowAddress = startCellAddress.getRow + rowIndex
      val row = Option(sheet.getRow(rowAddress)).getOrElse(sheet.createRow(rowAddress))
      tableData(rowIndex).indices.foreach { columnIndex =>
        val columnAddress = startCellAddress.getColumn + columnIndex
        val cell = Option(row.getCell(columnAddress)).getOrElse(row.createCell(columnAddress))
        writeCellValue(cell, tableData(rowIndex)(columnIndex))
        Option(columnStyles(columnIndex)).foreach(cs => cell.setCellStyle(cs.cellStyle))
      }
    }
    updateConditionalFormattingRanges(columnStyles, startCellAddress, tableData.size)
  }

  private def updateConditionalFormattingRanges(columnStyles: Seq[ComplexStyle], startCellAddress: CellAddress,
      rowsCount: Int): Unit = {
    columnStyles.zipWithIndex.foreach {
      case _ if rowsCount <= 0 => // pass
      case (null, _) => // pass
      case (columnStyle, columnIndex) =>
        columnStyle.conditionalFormatting.foreach { cf =>
          val startRow = startCellAddress.getRow
          val startColumn = startCellAddress.getColumn + columnIndex
          val cellRangeAddress = new CellRangeAddress(startRow, startRow + rowsCount - 1, startColumn, startColumn)
          cf.setFormattingRanges(cf.getFormattingRanges :+ cellRangeAddress)
        }
    }
  }

  private def writeCellValue(cell: Cell, value: Any): Unit = {
    value match {
      case null => cell.setBlank()
      case v: Boolean => cell.setCellValue(v)
      case v: Byte => cell.setCellValue(v.toDouble)
      case v: Short => cell.setCellValue(v.toDouble)
      case v: Int => cell.setCellValue(v.toDouble)
      case v: Long => cell.setCellValue(v.toDouble)
      case v: Float => cell.setCellValue(v.toDouble)
      case v: Double => cell.setCellValue(v)
      case v: String => cell.setCellValue(v)
//      case v: String =>
//        if (v.startsWith("=")) {
//          cell.setCellFormula(v.drop(1))
//        } else {
//          cell.setCellValue(v)
//          if (v.contains("\n")) {
//            // Can we want to edit the style here?
////            cell.getCellStyle.setWrapText(true)
//          }
//        }
      case v: BigDecimal => cell.setCellValue(v.toDouble)
      case v: java.math.BigDecimal => cell.setCellValue(v.doubleValue())
      case v: java.util.Date => cell.setCellValue(v)
//      case v: java.sql.Date => cell.setCellValue(v)
//      case v: java.sql.Timestamp => cell.setCellValue(v)
      case v: java.time.LocalDate => cell.setCellValue(java.sql.Date.valueOf(v))
      case v: java.time.Instant => cell.setCellValue(java.sql.Timestamp.from(v))
      // https://spark.apache.org/docs/latest/sql-reference.html#data-types
      // TODO: Does it make sense to somehow support Array[Byte], Seq, Map, Row?
      case v => throw new SToysException(s"Unsupported data type ${v.getClass.getName}.")
    }
  }

  private def processSpecialColumns(
      columnInfo: Seq[ColumnInfo], tableData: Seq[Seq[Any]]): (Seq[ColumnInfo], Seq[Seq[Any]]) = {
    val specialColumnInfo = getSpecialColumnInfo(columnInfo)
    val specialColumnInfoByName = specialColumnInfo.map(sci => sci.key -> sci).toMap
    if (specialColumnInfoByName.isEmpty) {
      (columnInfo, tableData)
    } else {
      val rowIdColumnIndex = specialColumnInfoByName.get("ROWID").map(_.index)
      val sortedTableData = rowIdColumnIndex
          .map(rici => tableData.sortBy(row => row(rici).asInstanceOf[java.lang.Long])).getOrElse(tableData)
      val specialColumnNames = specialColumnInfo.map(_.columnInfo.columnName).toSet
      val specialColumnMask = columnInfo.map(_.columnName).map(specialColumnNames)
      val processedColumnInfo = filterByMask(columnInfo, specialColumnMask)
      val processedTableData = sortedTableData.map(row => filterByMask(row, specialColumnMask))
      (processedColumnInfo, processedTableData)
    }
  }

  private def convertTableData(columnInfo: Seq[ColumnInfo], tableData: Seq[Seq[Any]]): Seq[Seq[Any]] = {
    val converters = columnInfo.map(_.converter)
    tableData.map(row => row.zip(converters).map(rc => rc._2.map(c => c(rc._1)).getOrElse(rc._1)))
  }

  private case class SpecialColumnInfo(
      key: String,
      index: Int,
      columnInfo: ColumnInfo
  )

  private def getSpecialColumnInfo(columnInfo: Seq[ColumnInfo]): Seq[SpecialColumnInfo] = {
    columnInfo.zipWithIndex.flatMap {
      case (columnInfo, index) =>
        columnInfo.columnName match {
          case SPECIAL_COLUMN_NAME_PATTERN(key) =>
            Some(SpecialColumnInfo(key.toUpperCase, index, columnInfo))
          case _ => None
        }
    }
  }

  private def filterByMask[T](values: Seq[T], mask: Seq[Boolean]): Seq[T] = {
    values.zip(mask).filterNot(_._2).map(_._1)
  }

  private def computeMaxColumnStringWidths(tableData: Seq[Seq[Any]], columnCount: Int): Seq[Int] = {
    tableData.foldLeft(Seq.fill(columnCount)(0)) {
      case (maxStringWidths, row) =>
        maxStringWidths.zip(row).map {
          case (maxStringWidth, value) if value == null => maxStringWidth
          case (maxStringWidth, value) => Math.max(maxStringWidth, value.toString.length)
        }
    }
  }

  private def autoSizeColumnsByMaxStringWidth(sheet: Sheet, startAddress: CellAddress, columnInfo: Seq[ColumnInfo],
      tableData: Seq[Seq[Any]], config: ExcelWriterAutoSizeConfig): Unit = {
    val columnWidths = columnInfo.zip(computeMaxColumnStringWidths(tableData, columnInfo.length)).map {
      case (columnInfo, maxColumnStringWidth) =>
        val columnNameWidth = Math.min(columnInfo.columnName.length, config.max_column_name_width)
        val combinedWidth = Math.max(columnNameWidth, maxColumnStringWidth)
        Math.max(Math.min(combinedWidth, config.max_column_width), config.min_column_width)
    }
    columnInfo.indices.foreach { i =>
      val width = (columnWidths(i) * config.scaling_factor).toInt * UNITS_PER_CHARACTER_WIDTH
      sheet.setColumnWidth(startAddress.getColumn + i, width)
    }
  }

  def serializeWorkbookToByteArray(workbook: Workbook): Array[Byte] = {
    IO.using(new ByteArrayOutputStream()) { baos =>
      workbook.write(baos)
      baos.toByteArray
    }
  }
}

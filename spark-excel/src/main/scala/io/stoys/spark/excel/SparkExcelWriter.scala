package io.stoys.spark.excel

import io.stoys.scala.Configuration
import io.stoys.spark.SToysException
import io.stoys.spark.datasources.BinaryFilePerRow
import io.stoys.spark.excel.ExcelWriter.ColumnInfo
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}

object SparkExcelWriter {
  def datasetToExcelFilesPerRow(
      dataset: Dataset[_], config: ExcelWriterConfig = ExcelWriterConfig.default): Dataset[BinaryFilePerRow] = {
    datasetsToExcelFilesPerRow(Seq(dataset), config)
  }

  def datasetsToExcelFilesPerRow(
      datasets: Seq[Dataset[_]], config: ExcelWriterConfig = ExcelWriterConfig.default): Dataset[BinaryFilePerRow] = {
    implicit val binaryFilePerRowEncoder: Encoder[BinaryFilePerRow] = ExpressionEncoder[BinaryFilePerRow]()
    val sheetRowsDfs = datasets.map(datasetToSheetRows)
    val multiSheetRowsDf = sheetRowsToMultiSheetRows(sheetRowsDfs)
    multiSheetRowsDf.map { row =>
      BinaryFilePerRow(row.getAs[String]("__path__"), multiSheetRowToExcelByteArray(row, config))
    }
  }

  def toExcel(row: Row, configMap: Map[String, Any]): Array[Byte] = {
    val config = Configuration.updateCaseClassWithConfigMap(ExcelWriterConfig.default, configMap)
    workbookRowToExcelByteArray(row, config)
  }

  def workbookRowToExcelByteArray(row: Row, config: ExcelWriterConfig): Array[Byte] = {
    val workbook = ExcelWriter.createWorkbook(config)
    row.schema.fields.zipWithIndex.foreach {
      case (StructField(sheetName, ArrayType(StructType(fields), _), _, _), index) =>
        val columnInfo = getColumnInfo(fields, config)
        val rows = row.getSeq[Row](index).map(_.toSeq)
        ExcelWriter.writeTableDataIntoWorkbook(workbook, sheetName, columnInfo, rows, config)
      case (sf, _) if ExcelWriter.SPECIAL_COLUMN_NAME_PATTERN.findFirstIn(sf.name).isDefined => // ignore
      case (sf, _) => throw new SToysException(s"Struct field '$sf' is not supported. " +
          s"All columns types given to_excel have to be array of flat structs.")
    }
    ExcelWriter.serializeWorkbookToByteArray(workbook)
  }

  private def datasetToSheetRows(ds: Dataset[_]): DataFrame = {
    val allPossibleGroupByColumnNames = Set("__path__", "__sheet__")
    val groupByColumnNames = ds.columns.filter(allPossibleGroupByColumnNames)
    val dataColumns = ds.schema.fieldNames.filter(fn => !groupByColumnNames.contains(fn)).map(fn => col(fn))
    val indexedDf = ds.withColumn("__rowid__", monotonically_increasing_id())
    indexedDf.groupBy(groupByColumnNames.map(col): _*).agg(collect_list(struct(dataColumns: _*)).as("__rows__"))
  }

  private def sheetRowsToMultiSheetRows(dfs: Seq[DataFrame]): DataFrame = {
    val dfsWithSheets = dfs.zipWithIndex.map {
      case (df, _) if df.columns.contains("__sheet__") => df
      case (df, index) =>
        df.withColumn("__sheet__", lit(s"Sheet${index + 1}"))
    }
    val (dfsWithPath, dfsWithoutPath) = dfsWithSheets.partition(_.columns.contains("__path__"))
    val finalDfs = dfsWithPath ++ dfsWithoutPath
    val dfsNames = finalDfs.indices.map(i => s"__df_${i}__")
    val aliasedDfs = finalDfs.zip(dfsNames).map(dfn => dfn._1.as(dfn._2))
    val (aliasedDfsWithPath, aliasedDfsWithoutPath) = aliasedDfs.splitAt(dfsWithPath.size)
    val joinedDfsWithPath = aliasedDfsWithPath.reduceOption((a, b) => a.join(b, Seq("__path__"), "FULL"))
    val joinedDfsWithoutPath = aliasedDfsWithoutPath.reduceOption((a, b) => a.join(b, lit(true), "CROSS"))
    val joinedDfs = (joinedDfsWithPath, joinedDfsWithoutPath) match {
      // TODO: Is there a way to create empty DataFrame without requiring sparkSession in the api?
      case (None, None) => throw new SToysException("At least one dataset required for this function.")
      case (Some(dfWithPath), None) => dfWithPath
      case (None, Some(dfWithoutPath)) => dfWithoutPath.withColumn("__path__", lit("workbook.xlsx"))
      case (Some(dfWithPath), Some(dfWithoutPath)) => dfWithPath.join(dfWithoutPath, lit(true), "CROSS")
    }
    val sheetArrayColumn = array(dfsNames.map(n => col(s"$n.__sheet__")): _*).as("__sheet_names__")
    val rowsColumns = dfsNames.zipWithIndex.map(ni => col(s"${ni._1}.__rows__").as(s"__rows_${ni._2}__"))
    joinedDfs.select(col("__path__") +: sheetArrayColumn +: rowsColumns: _*)
  }

  private def multiSheetRowToExcelByteArray(row: Row, config: ExcelWriterConfig): Array[Byte] = {
    val workbook = ExcelWriter.createWorkbook(config)
    val sheetNames = row.getSeq[String](row.fieldIndex("__sheet_names__"))
    sheetNames.zipWithIndex.foreach {
      case (sheetName, index) =>
        val rowsFieldIndex = row.fieldIndex(s"__rows_${index}__")
        (row.getSeq[Row](rowsFieldIndex), row.schema.fields(rowsFieldIndex).dataType) match {
          case (null, _) => // __sheet__ has no rows for given __path__
          case (rows, ArrayType(StructType(fields), _)) =>
            val columnInfo = getColumnInfo(fields, config)
            ExcelWriter.writeTableDataIntoWorkbook(workbook, sheetName, columnInfo, rows.map(_.toSeq), config)
          case (_, dt) => throw new SToysException(s"Unsupported __rows__ field data type '$dt'.")
        }
    }
    ExcelWriter.serializeWorkbookToByteArray(workbook)
  }

  private def getColumnInfo(fields: Seq[StructField], config: ExcelWriterConfig): Seq[ColumnInfo] = {
    fields.map { field =>
      field.dataType match {
        case _: LongType if field.name.endsWith("_cents") && config.convert_cents_to_currency =>
          val centsConverter = (value: Any) => value.asInstanceOf[java.lang.Long].toDouble / 100.0
          ColumnInfo(field.name.stripSuffix("_cents"), Some(config.currency_format), converter = Some(centsConverter))
        case _: DateType => ColumnInfo(field.name, Some(config.date_format))
        case _: TimestampType => ColumnInfo(field.name, Some(config.timestamp_format))
        case _: BooleanType | _: DateType | _: NullType | _: NumericType | _: StringType | _: TimestampType =>
          ColumnInfo(field.name, None)
        case _ if config.convert_unsupported_types_to_string =>
          ColumnInfo(field.name, None, converter = Some(_.toString))
        case dt => throw new SToysException(s"Column type '$dt' is not supported. " +
            s"${this.getClass.getSimpleName} does not support nested, unusual nor custom column types.")
      }
    }
  }
}

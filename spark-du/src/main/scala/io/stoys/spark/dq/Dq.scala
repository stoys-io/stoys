package io.stoys.spark.dq

import io.stoys.spark.SToysException
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class Dq[T] private(ds: Dataset[T], rulesWithinDs: Seq[DqRule]) {
  private val logger = org.log4s.getLogger

  private var config: DqConfig = DqConfig.default
  private var fields: Seq[DqField] = Seq.empty
  private var metadata: Map[String, String] = Map.empty
  private var primaryKeyFieldNames: Seq[String] = Seq.empty
  private var rules: Seq[DqRule] = Seq.empty

  def config(config: DqConfig): Dq[T] = {
    this.config = config
    this
  }

  def fields(fields: Seq[DqField]): Dq[T] = {
    this.fields ++= fields
    this
  }

  def metadata(metadata: Map[String, String]): Dq[T] = {
    this.metadata ++= metadata
    this
  }

  def primaryKeyFieldNames(primaryKeyFieldNames: Seq[String]): Dq[T] = {
    this.primaryKeyFieldNames = primaryKeyFieldNames
    this
  }

  def rules(rules: Seq[DqRule]): Dq[T] = {
    this.rules ++= rules
    this
  }

  def computeDqResult(): Dataset[DqResult] = {
    val wideDqDfInfo = computeWideDqDfInfo()
    val columnNames = wideDqDfInfo.wideDqDf.columns.toSeq.dropRight(wideDqDfInfo.ruleInfo.size)
    DqFramework.computeDqResult(wideDqDfInfo.wideDqDf, columnNames, wideDqDfInfo.ruleInfo, config, metadata)
  }

  def computeDqViolationPerRow(): Dataset[DqViolationPerRow] = {
    if (primaryKeyFieldNames.isEmpty) {
      val className = classOf[DqViolationPerRow].getSimpleName
      logger.warn(s"$className should probably have primaryKeyFieldNames configured!")
    }
    val wideDqDfInfo = computeWideDqDfInfo()
    DqFramework.computeDqViolationPerRow(wideDqDfInfo, primaryKeyFieldNames)
  }

  def selectFailingRows(): DataFrame = {
    val wideDqDfInfo = computeWideDqDfInfo()
    val cleanWideDqDf = wideDqDfInfo.wideDqDf.drop(DqFile.corruptRecordField.name)
    DqFramework.selectFailingRows(cleanWideDqDf, wideDqDfInfo.ruleInfo.size - rulesWithinDs.size)
  }

  private def getSchemaRules: Seq[DqRule] = {
    DqSchema.generateSchemaRules(ds.schema, fields, primaryKeyFieldNames, config)
  }

  private def computeWideDqDfInfo(): DqFramework.WideDqDfInfo = {
    DqFramework.computeWideDqDfInfo(ds, rulesWithinDs, rules ++ getSchemaRules)
  }
}

object Dq {
  def fromDataset[T](ds: Dataset[T]): Dq[T] = {
    new Dq(ds, Seq.empty)
  }

  def fromDqSql(sparkSession: SparkSession, dqSql: String): Dq[Row] = {
    val parsedDqSql = DqSql.parseDqSql(sparkSession, dqSql)
    val missingReferencedTableNames = parsedDqSql.referencedTableNames.filterNot(sparkSession.catalog.tableExists)
    if (missingReferencedTableNames.nonEmpty) {
      throw new SToysException(s"Dq sql reference missing tables: ${missingReferencedTableNames.toList}")
    }
    val wideDqDf = sparkSession.sql(dqSql)
    new Dq(wideDqDf, parsedDqSql.rules)
  }

  def fromFileInputPath(sparkSession: SparkSession, inputPath: String): Dq[Row] = {
    val fileInput = DqFile.openFileInputPath(sparkSession, inputPath)
    new Dq(fileInput.df, Seq.empty).rules(fileInput.rules).metadata(fileInput.metadata)
  }

  def fromTableName(sparkSession: SparkSession, tableName: String): Dq[Row] = {
    if (!sparkSession.catalog.tableExists(tableName)) {
      throw new SToysException(s"Table '$tableName' does not exist in current spark session.")
    }
    new Dq(sparkSession.table(tableName), Seq.empty).metadata(Map("table_name" -> tableName))
  }
}

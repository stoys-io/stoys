package io.stoys.spark

import io.stoys.scala.Jackson
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession}

import scala.collection.mutable

// Note: You might want to call registerAllInputTables.
class SparkIO(sparkSession: SparkSession, config: SparkIOConfig) extends AutoCloseable {
  import InputPathResolver._
  import SparkIO._

  private val logger = org.log4s.getLogger

  private val dfs = Dfs(sparkSession)
  private val inputPathResolver = new InputPathResolver(dfs)

  private val inputPaths = mutable.Buffer.empty[String]
  private val inputDags = mutable.Buffer.empty[DagInfo]
  private val inputTables = mutable.Map.empty[String, TableInfo]
  private val outputTables = mutable.Map.empty[String, TableInfo]

  private val inputDataFrames = mutable.Map.empty[String, DataFrame]

  config.input_paths.foreach(addInputPath)

  def addInputPath(path: String): Unit = {
    inputPaths.append(path)
    inputPathResolver.resolveInputs(path).foreach {
      case dag: DagInfo => inputDags.append(dag)
      case table: TableInfo => addInputTable(table)
    }
  }

  private def addInputTable(tableInfo: TableInfo): Unit = {
    getInputTable(tableInfo.tableName) match {
      case Some(existingTable) if existingTable == tableInfo =>
        logger.debug(s"Resolved the same table again: $tableInfo")
      case Some(existingTable) =>
        throw new SToysException(s"Resolved conflicting tables `${tableInfo.tableName}`: $existingTable vs $tableInfo")
      case None =>
        inputTables.put(tableInfo.tableName, tableInfo)
        if (config.register_input_tables) {
          registerInputTable(tableInfo.tableName)
        }
    }
  }

  def listInputTableNames(): Set[String] = {
    inputTables.keys.toSet
  }

  def getInputTable(fullTableName: String): Option[TableInfo] = {
    inputTables.get(fullTableName)
  }

  private def getOrLoadDataFrame(fullTableName: String): DataFrame = {
    getInputTable(fullTableName) match {
      case Some(inputTable) =>
        inputDataFrames.getOrElseUpdate(fullTableName, {
          logger.debug(s"Loading `$fullTableName` ...")
          createDataFrameReader(sparkSession, inputTable).load(inputTable.path).as(fullTableName)
        })
      case None =>
        throw new SToysException(s"No table `$fullTableName` has been added yet!")
    }
  }

  private def registerInputTable(fullTableName: String): Unit = {
    if (!sparkSession.catalog.tableExists(fullTableName)) {
      logger.debug(s"Registering temp view `$fullTableName` ...")
      getOrLoadDataFrame(fullTableName).createOrReplaceTempView(fullTableName)
    }
  }

  def df[T <: Product](tableName: TableName[T]): DataFrame = {
    getOrLoadDataFrame(tableName.fullTableName())
  }

  def ds[T <: Product](tableName: TableName[T]): Dataset[T] = {
    // TODO: the reshaping should be part of the cache but it requires to scan entity classes first
    val dataFrame = df(tableName)
    val reshapeConfig = Jackson.json.updateValue(config.input_reshape_config.copy(),
      getInputTable(tableName.fullTableName()).map(_.reshapeConfigPropsOverrides))
    Reshape.reshape[T](dataFrame, reshapeConfig)(tableName.typeTag).as(tableName.fullTableName())
  }

  private def writeDF(df: DataFrame, fullTableName: String,
      format: Option[String], writeMode: Option[String], options: Map[String, String]): Unit = {
    val path = s"${config.output_path.get}/$fullTableName"
    if (outputTables.contains(fullTableName)) {
      throw new SToysException(s"Writing table `$fullTableName` multiple times!")
    } else {
      logger.debug(s"Writing `$fullTableName` to $path ...")
      val writer = df.write
      format.foreach(writer.format)
      writeMode.foreach(writer.mode)
      writer.options(options)
      writer.save(path)
      outputTables.put(fullTableName, TableInfo(path, fullTableName, format, Map.empty, options))
    }
  }

  def write[T <: Product](ds: Dataset[T], tableName: TableName[T]): Unit = {
    write[T](ds, tableName, config.write_format, config.write_mode, config.write_options)
  }

  def write[T <: Product](ds: Dataset[T], tableName: TableName[T],
      format: Option[String], writeMode: Option[String], options: Map[String, String]): Unit = {
    writeDF(ds.toDF(), tableName.fullTableName(), format, writeMode, options)
  }

  override def close(): Unit = {
    config.output_path.foreach { outputPath =>
      dfs.writeString(s"$outputPath/$DAG_DIR/input_paths.list", inputPaths.mkString("\n"))
      dfs.writeString(s"$outputPath/$DAG_DIR/input_dags.list", inputDags.map(_.toInputPathUrl).mkString("\n"))
      val inputTablesContent = inputTables.map(_._2.toInputPathUrl).toSeq.sorted.mkString("\n")
      dfs.writeString(s"$outputPath/$DAG_DIR/input_tables.list", inputTablesContent)
      val outputTablesContent = outputTables.map(_._2.toInputPathUrl).toSeq.sorted.mkString("\n")
      dfs.writeString(s"$outputPath/$DAG_DIR/output_tables.list", outputTablesContent)
    }
  }

  def writeSymLink(path: String): Unit = {
    config.output_path.foreach(outputPath => dfs.writeString(path, DagInfo(outputPath).toInputPathUrl))
  }
}

object SparkIO {
  import InputPathResolver._

  private[spark] def createDataFrameReader(sparkSession: SparkSession, inputTable: TableInfo): DataFrameReader = {
    var reader = sparkSession.read
    inputTable.format.foreach(f => reader = reader.format(f))
    reader = reader.options(inputTable.sparkOptions)
    reader
  }
}

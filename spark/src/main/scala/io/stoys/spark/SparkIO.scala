package io.stoys.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession}

import scala.collection.mutable

// Note: Do not forget to call init() first to register config.inputPaths!
class SparkIO(sparkSession: SparkSession, config: SparkIOConfig) extends AutoCloseable {
  import InputPathResolver._
  import SparkIO._

  private val logger = org.log4s.getLogger

  private val dfs = Dfs(sparkSession)
  private val inputPathResolver = new InputPathResolver(dfs)

  private val inputPaths = mutable.Buffer.empty[String]
  private val inputDags = mutable.Buffer.empty[SosDag]
  private val inputTables = mutable.Map.empty[String, SosTable]
  private val outputTables = mutable.Map.empty[String, SosTable]

  def init(): Unit = {
    addInputPaths(config.inputPaths: _*)
  }

  private def addInputTable(table: SosTable): Unit = {
    inputTables.get(table.table_name) match {
      case Some(existingTable) if existingTable == table =>
        logger.debug(s"Resolved the same table again ($table).")
      case Some(existingTable) =>
        val msg = s"Resolved conflicting tables for table_name ${table.table_name} ($existingTable and $table)!"
        logger.error(msg)
        throw new SToysException(msg)
      case None =>
        inputTables.put(table.table_name, table)
        registerInputTable(table)
    }
  }

  def addInputPaths(paths: String*): Unit = {
    paths.foreach { path =>
      inputPaths.append(path)
      inputPathResolver.resolveInputs(path).foreach {
        case dag: SosDag => inputDags.append(dag)
        case table: SosTable => addInputTable(table)
      }
    }
  }

  def df[T <: Product](tableName: TableName[T]): DataFrame = {
    sparkSession.table(tableName.fullTableName())
  }

  def ds[T <: Product](tableName: TableName[T]): Dataset[T] = {
    Reshape.reshape[T](df(tableName), config.inputReshapeConfig)(tableName.typeTag)
  }

  private def writeDF(df: DataFrame, fullTableName: String,
      format: Option[String], writeMode: Option[String], options: Map[String, String]): Unit = {
    val path = s"${config.outputPath.get}/$fullTableName"
    val table = SosTable(fullTableName, path, format, options)
    if (outputTables.contains(table.table_name)) {
      val msg = s"Writing table with the same table_name ${table.table_name} multiple times!"
      logger.error(msg)
      throw new SToysException(msg)
    } else {
      logger.info(s"Writing $fullTableName to $path.")
      val writer = df.write
      table.format.foreach(writer.format)
      writeMode.foreach(writer.mode)
      writer.options(table.options)
      writer.save(path)
      outputTables.put(table.table_name, table)
    }
  }

  def write[T <: Product](ds: Dataset[T], tableName: TableName[T]): Unit = {
    write[T](ds, tableName, config.writeFormat, config.writeMode, config.writeOptions)
  }

  def write[T <: Product](ds: Dataset[T], tableName: TableName[T],
      format: Option[String], writeMode: Option[String], options: Map[String, String]): Unit = {
    writeDF(ds.toDF(), tableName.fullTableName(), format, writeMode, options)
  }

  private def registerInputTable(inputTable: SosTable): DataFrame = {
    logger.info(s"Registering input table $inputTable")
    val table = createDataFrameReader(sparkSession, inputTable).load(inputTable.path)
    table.createOrReplaceTempView(inputTable.table_name)
    table
  }

  override def close(): Unit = {
    config.outputPath.foreach { outputPath =>
      dfs.writeString(s"$outputPath/$DAG_DIR/input_paths.list", inputPaths.mkString("\n"))
      dfs.writeString(s"$outputPath/$DAG_DIR/input_dags.list", inputDags.map(_.toUrlString).mkString("\n"))
      val inputTablesContent = inputTables.map(_._2.toUrlString).toSeq.sorted.mkString("\n")
      dfs.writeString(s"$outputPath/$DAG_DIR/input_tables.list", inputTablesContent)
      val outputTablesContent = outputTables.map(_._2.toUrlString).toSeq.sorted.mkString("\n")
      dfs.writeString(s"$outputPath/$DAG_DIR/output_tables.list", outputTablesContent)
    }
  }

  def getInputTable(fullTableName: String): Option[SosTable] = {
    inputTables.get(fullTableName)
  }

  def writeSymLink(path: String): Unit = {
    config.outputPath.foreach(outputPath => dfs.writeString(path, SosDag(outputPath).toUrlString))
  }
}

object SparkIO {
  import InputPathResolver._

  private[spark] def createDataFrameReader(sparkSession: SparkSession, inputTable: SosTable): DataFrameReader = {
    var reader = sparkSession.read
    inputTable.format.foreach(f => reader = reader.format(f))
    reader = reader.options(inputTable.options)
    reader
  }
}

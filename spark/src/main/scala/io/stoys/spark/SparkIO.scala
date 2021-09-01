package io.stoys.spark

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
  private val inputDags = mutable.Buffer.empty[SosDag]
  private val inputTables = mutable.Map.empty[String, SosTable]
  private val outputTables = mutable.Map.empty[String, SosTable]

  private val inputDataFrames = mutable.Map.empty[String, DataFrame]

  config.input_paths.foreach(addInputPath)

  def addInputPath(path: String): Unit = {
    inputPaths.append(path)
    inputPathResolver.resolveInputs(path).foreach {
      case dag: SosDag => inputDags.append(dag)
      case table: SosTable => addInputTable(table)
    }
  }

  private def addInputTable(table: SosTable): Unit = {
    inputTables.get(table.table_name) match {
      case Some(existingTable) if existingTable == table =>
        logger.debug(s"Resolved the same table again: $table")
      case Some(existingTable) =>
        throw new SToysException(s"Resolved conflicting tables `${table.table_name}`: $existingTable vs $table")
      case None =>
        inputTables.put(table.table_name, table)
        if (config.register_input_tables) {
          registerInputTable(table.table_name)
        }
    }
  }

  def listInputTableNames(): Set[String] = {
    inputTables.keys.toSet
  }

  def getInputTable(fullTableName: String): Option[SosTable] = {
    inputTables.get(fullTableName)
  }

  private def getOrLoadInputTable(fullTableName: String): DataFrame = {
    inputTables.get(fullTableName) match {
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
      getOrLoadInputTable(fullTableName).createOrReplaceTempView(fullTableName)
    }
  }

//  def registerAllInputTables(): Unit = {
//    inputTables.keys.foreach(registerInputTable)
//  }

  def df[T <: Product](tableName: TableName[T]): DataFrame = {
    getOrLoadInputTable(tableName.fullTableName())
  }

  def ds[T <: Product](tableName: TableName[T]): Dataset[T] = {
    Reshape.reshape[T](df(tableName), config.input_reshape_config)(tableName.typeTag)
  }

  private def writeDF(df: DataFrame, fullTableName: String,
      format: Option[String], writeMode: Option[String], options: Map[String, String]): Unit = {
    val path = s"${config.output_path.get}/$fullTableName"
    val table = SosTable(fullTableName, path, format, options)
    if (outputTables.contains(table.table_name)) {
      throw new SToysException(s"Writing table `${table.table_name}` multiple times!")
    } else {
      logger.debug(s"Writing `$fullTableName` to $path ...")
      val writer = df.write
      table.format.foreach(writer.format)
      writeMode.foreach(writer.mode)
      writer.options(table.options)
      writer.save(path)
      outputTables.put(table.table_name, table)
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
      dfs.writeString(s"$outputPath/$DAG_DIR/input_dags.list", inputDags.map(_.toUrlString).mkString("\n"))
      val inputTablesContent = inputTables.map(_._2.toUrlString).toSeq.sorted.mkString("\n")
      dfs.writeString(s"$outputPath/$DAG_DIR/input_tables.list", inputTablesContent)
      val outputTablesContent = outputTables.map(_._2.toUrlString).toSeq.sorted.mkString("\n")
      dfs.writeString(s"$outputPath/$DAG_DIR/output_tables.list", outputTablesContent)
    }
  }

  def writeSymLink(path: String): Unit = {
    config.output_path.foreach(outputPath => dfs.writeString(path, SosDag(outputPath).toUrlString))
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

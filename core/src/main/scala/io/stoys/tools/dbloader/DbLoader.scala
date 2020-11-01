package io.stoys.tools.dbloader

import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import io.stoys.core.SparkConfig
import io.stoys.core.spark.{SparkIO, SparkIOConfig, SparkUtils, TableName, TableNameLookup}
import io.stoys.core.util.{Configuration, IO, Reflection, Strings}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}

class DbLoader(args: Array[String]) {
  private val logger = org.log4s.getLogger

  val configuration = Configuration(args)
  val sparkConfig = configuration.readConfig[SparkConfig]
  val sparkIOConfig = configuration.readConfig[SparkIOConfig]
  val config = configuration.readConfig[DbLoaderConfig]

  val explicitTimestamp = Strings.trim(config.timestamp).map(DbLoader.TIMESTAMP_FORMATTER.parse)
  val timestampInEpochS = explicitTimestamp.getOrElse(Instant.now())
  val timestamp = DbLoader.TIMESTAMP_FORMATTER.format(timestampInEpochS)
  val timestampParam = Map("timestamp" -> timestamp)
  assert(Strings.trim(config.schemaName).isDefined, "Please specify schema_name!")
  val schemaName = Strings.replaceParams(config.schemaName, params = timestampParam)
  val params = timestampParam ++ Map("schema_name" -> schemaName)

  val jdbcOptions = replaceParams(config.jdbcOptions, JDBCOptions.JDBC_SESSION_INIT_STATEMENT)
  val writeOptions = replaceParams(config.sparkWriteOptions, JDBCOptions.JDBC_SESSION_INIT_STATEMENT)
  val jdbcProperties = new java.util.Properties()
  jdbcOptions.foreach(kv => jdbcProperties.put(kv._1, kv._2))
  config.jdbcUser.map(v => jdbcProperties.put("user", v))
  config.jdbcPassword.map(v => jdbcProperties.put("password", v))

  val executedSqlStatements = Seq.newBuilder[String]

  def run(): Unit = {
    val sparkSession = SparkUtils.createSparkSession(sparkConfig)
    val sparkIO = new SparkIO(sparkSession, sparkIOConfig)
    sparkIO.init()

    IO.using(DriverManager.getConnection(config.jdbcUrl, jdbcProperties)) { connection =>
      runDbSqlFile(connection, config.beforeLoadScript, params = params)
      if (!config.disableSchemaCreation) {
        logger.info(s"Creating schema $schemaName.")
        runDbSql(connection, s"CREATE SCHEMA $schemaName")
      }
      val tableNameLookup = new TableNameLookup(lookupClasses(config.caseClassNames))
      config.tableNames.foreach { fullTableName =>
        tableNameLookup.parse(fullTableName) match {
          case Some(tableName) => writeTable(tableName, sparkIO, connection)
          case None => logger.error(s"Unable to lookup TableName for '$fullTableName'")
        }
      }
      runDbSqlFile(connection, config.afterLoadScript, params = params)
    }

    if (config.executedSqlOutputFile.isDefined) {
      val fileName = config.executedSqlOutputFile.get
      logger.info(s"Writing executed sql statements to $fileName.")
      Files.write(Paths.get(fileName), executedSqlStatements.result().mkString("", ";\n\n", ";\n").getBytes())
    }
  }

  def writeTable[T <: Product](tableName: TableName[T], sparkIO: SparkIO, connection: Connection): Unit = {
    val qualifiedTableName = JdbcReflection.getQualifiedTableName[T](tableName, schemaName)

    if (!config.disableTableCreation) {
      logger.info(s"Creating table $qualifiedTableName.")
      runDbSql(connection, JdbcReflection.getCreateTableStatement[T](tableName, schemaName))
    }

    if (config.limit.isEmpty || config.limit.getOrElse(0) > 0) {
      logger.info(s"Writing $qualifiedTableName:")
      val table = normalizeTable[T](sparkIO.df(tableName))(tableName.typeTag)
      val limited = config.limit.map(table.limit).getOrElse(table)
      limited.write.mode(SaveMode.Append).options(writeOptions).jdbc(config.jdbcUrl, qualifiedTableName, jdbcProperties)
    }

    if (!config.disableConstrainCreation) {
      logger.info(s"Creating constrains for table $qualifiedTableName.")
      val constraints = JdbcReflection.getAddConstraintStatements[T](tableName, schemaName)
      constraints.foreach(constraint => runDbSql(connection, constraint))
    }
  }

  def replaceParams(options: Map[String, String], keys: String*): Map[String, String] = {
    val overrides = options.filterKeys(keys.contains).mapValues(v => Strings.replaceParams(v, params))
    options ++ overrides
  }

  def runDbSqlFile(connection: Connection, fileName: String, params: Map[String, Any] = Map.empty): Unit = {
    if (Strings.trim(fileName).isDefined) {
      logger.info(s"Running sql script $fileName.")
      val sql = IO.resourceToString(this.getClass, fileName)
      runDbSql(connection, sql, params)
    }
  }

  def runDbSql(connection: Connection, sql: String, params: Map[String, Any] = Map.empty): Unit = {
    // TODO: Can we do proper splitting and comments removing which actually understand sql language?
    val rawSqlStatements = sql.split(";").map(l => Strings.unsafeRemoveLineComments(l, "--")).flatMap(Strings.trim)
    IO.using(connection.createStatement()) { statement =>
      rawSqlStatements.foreach { rawSqlStatement =>
        val sqlStatement = Strings.replaceParams(rawSqlStatement, params)
        statement.execute(sqlStatement)
        executedSqlStatements += sqlStatement
      }
    }
  }

  def normalizeTable[T: TypeTag](df: DataFrame): DataFrame = {
    val expectedColumnNames = Reflection.getCaseClassFieldNames[T]
    val normalizedColumns = df.schema.filter(field => expectedColumnNames.contains(field.name)).map { field =>
      val normalizedName = Strings.toSnakeCase(field.name)
      val normalizedColumn = field.dataType match {
        case _: ArrayType | _: MapType | _: StructType => to_json(col(field.name))
        case _ => col(field.name)
      }
      normalizedColumn.as(normalizedName)
    }
    df.select(normalizedColumns: _*)
  }

  private def lookupClasses[T](classNames: Iterable[String]): Iterable[Class[T]] = {
    classNames.flatMap { className =>
      scala.util.Try(Class.forName(className)) match {
        case Success(clazz) =>
          Some(clazz.asInstanceOf[Class[T]])
        case Failure(e) =>
          logger.error(e)(s"Unable to find class $className on class path!")
          None
      }
    }
  }
}

object DbLoader {
  val TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC"))

  def main(args: Array[String]): Unit = {
    val dbLoader = new DbLoader(args)
    dbLoader.run()
  }
}

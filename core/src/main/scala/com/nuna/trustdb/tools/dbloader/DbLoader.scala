package com.nuna.trustdb.tools.dbloader

import java.sql.{Connection, DriverManager}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import com.nuna.trustdb.core.SparkConfig
import com.nuna.trustdb.core.spark.{SparkIO, SparkIOConfig, SparkUtils, TableName, TableNameLookup}
import com.nuna.trustdb.core.util.{Configuration, IO, Reflection, Strings}
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

  val explicitTimestamp = Strings.trim(config.timestamp).map(DbLoader.timestampFormatter.parse)
  val timestampInEpochS = explicitTimestamp.getOrElse(Instant.now())
  val timestamp = DbLoader.timestampFormatter.format(timestampInEpochS)
  val timestampParam = Map("timestamp" -> timestamp)
  assert(Strings.trim(config.schemaName).isDefined, "Please specify schema_name!")
  val schemaName = Strings.replaceParams(config.schemaName, params = timestampParam)
  val VERSION_DB = "version"
  val params = timestampParam ++ Map("schema_name" -> schemaName)

  val jdbcOptions = replaceParams(config.jdbcOptions, JDBCOptions.JDBC_SESSION_INIT_STATEMENT)
  val writeOptions = replaceParams(config.sparkWriteOptions, JDBCOptions.JDBC_SESSION_INIT_STATEMENT)
  val jdbcProperties = new java.util.Properties()
  jdbcOptions.foreach(p => jdbcProperties.put(p._1, p._2))
  config.username.map(v => jdbcProperties.put("user", v))
  config.password.map(v => jdbcProperties.put("password", v))

  def run(): Unit = {
    val sparkSession = SparkUtils.createSparkSession(sparkConfig)
    val sparkIO = new SparkIO(sparkSession, sparkIOConfig)

    IO.using(DriverManager.getConnection(config.jdbcUrl, jdbcProperties)) { connection =>
      runDbSqlFile(connection, config.beforeLoadScript, params = params)
      runDbSql(connection, s"CREATE SCHEMA $schemaName")
      val tableNameLookup = new TableNameLookup(lookupClasses(config.caseClassNames))
      config.tableNames.foreach { fullTableName =>
        tableNameLookup.parse(fullTableName) match {
          case Some(tableName) => writeTable(tableName, sparkIO, connection)
          case None => logger.error(s"Unable to lookup TableName for '$fullTableName'")
        }
      }
      runDbSqlFile(connection, config.afterLoadScript, params = params)
    }
  }

  def writeTable[T <: Product](tableName: TableName[T], sparkIO: SparkIO, connection: Connection): Unit = {
    val qualifiedTableName = JdbcReflection.getQualifiedTableName[T](tableName, schemaName)

    if (!config.disableTableCreation) {
      logger.info(s"Creating table $qualifiedTableName.")
      runDbSql(connection, JdbcReflection.getCreateTableStatement[T](tableName, schemaName))
    }

    logger.info(s"Writing $qualifiedTableName:")
    val table = normalizeTable[T](sparkIO.df(tableName))(tableName.typeTag)
    table.write.mode(SaveMode.Append).options(writeOptions).jdbc(config.jdbcUrl, qualifiedTableName, jdbcProperties)

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
      val sql = IO.readResource(this.getClass, fileName)
      runDbSql(connection, sql, params)
    }
  }

  def runDbSql(connection: Connection, sql: String, params: Map[String, Any] = Map.empty): Unit = {
    val statements = sql.split(";").map(l => Strings.unsafeRemoveLineComments(l, "--")).flatMap(Strings.trim)
    statements.foreach(println)
    IO.using(connection.createStatement()) { statement =>
      // TODO: Do we want to use mysql specific allowMultiQueries instead of hacky split on semicolon?
      statements.map(s => Strings.replaceParams(s, params)).foreach(statement.execute)
    }
  }

  def normalizeTable[T: TypeTag](df: DataFrame): DataFrame = {
    val expectedCols = Reflection.getCaseClassFields[T].map(_.name.decodedName.toString)
    val normalizedColumns = df.schema.filter(field => expectedCols.contains(field.name)).map { field =>
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
  val timestampFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC"))

  def main(args: Array[String]): Unit = {
    val dbLoader = new DbLoader(args)
    dbLoader.run()
  }
}

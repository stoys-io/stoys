package io.stoys.spark.db

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonProperty.Access
import io.stoys.scala.Params

@Params(allowInRootPackage = true)
case class DbLoaderConfig(
    // Table (directory) names to load.
    tableNames: Seq[String],
    // TODO: deprecate these and switch to classpath scanning.
    // DEPRECATED: Full name of case classes representing the tables.
    // Note: The classes have to be on classpath.
    caseClassNames: Seq[String],
    // Jdbc url of target database.
    jdbcUrl: String,
    // Jdbc user name.
    jdbcUser: Option[String],
    // Jdbc password.
    @JsonProperty(access = Access.WRITE_ONLY)
    jdbcPassword: Option[String],
    // Timestamp. Current time - now() - will be used if missing.
    timestamp: String,
    // Name of the schema to be created. For example: "data_${timestamp}" (the timestamp variable will get replaced).
    schemaName: String,
    // File name of sql script to run before all the tables are imported.
    beforeLoadScript: String,
    // File name of sql script to run after all the tables are imported.
    afterLoadScript: String,
    // Jdbc options.
    jdbcOptions: Map[String, String],
    // Spark write options.
    sparkWriteOptions: Map[String, String],
    // Limit number of rows to load per table. Note: Zero or less will completely skip spark and table loading.
    limit: Option[Int],
    // Disable table creation.
    disableSchemaCreation: Boolean,
    // Disable table creation by our JdbcReflection class. Spark may still create the table.
    // Note: Add your own "CREATE TABLE" statement(s) into beforeLoadScript for full control over the table creation.
    disableTableCreation: Boolean,
    // This primary key and unique key constrains creation. JdbcReflection creates primary key on @Id annotated columns.
    // The unique key constrains are generated per column if @Column(unique=true) is present.
    disableConstrainCreation: Boolean,
    // Write executed sql statements from DbLoader (not from Spark!) to a file.
    executedSqlOutputFile: Option[String],
)

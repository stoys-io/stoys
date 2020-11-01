package io.stoys.spark.db

import java.sql._

import io.stoys.scala.{IO, Jackson}
import io.stoys.spark.SparkTestBase
import org.apache.commons.text.StringEscapeUtils
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StringType}

class DbLoaderTest extends SparkTestBase {
  import DbLoaderTest._
  import sparkSession.implicits._

  JdbcDialects.registerDialect(SparkH2DialectWithJsonSupport)

  val items = Seq(
    Item(1, 11, "name1", 100, "note1", Some("optionalNote1"), "longNameField1", "customTypeField1",
      Date.valueOf("2001-01-01"), Seq("repeatedFiled11", "repeatedFiled12"), SubItem("nestedField1")),
    Item(2, 22, "name2", 200, "note2", Some("optionalNote2"), "longNameField2", "customTypeField2",
      Date.valueOf("2001-01-02"), Seq("repeatedFiled12", "repeatedFiled22"), SubItem("nestedField2")))
  val caseClassName = classOf[Item].getName
  val tableName = s"${classOf[Item].getSimpleName.toLowerCase}__latest"

  test("DbLoader") {
    val dbName = this.getClass.getSimpleName
    writeData(s"$tmpDir/$dbName/$tableName", items)

    val jdbcUrl = s"jdbc:h2:mem:$dbName;USER=sa;PASSWORD=;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_DELAY=-1;"
//    val jdbcUrl = s"jdbc:h2:file:$tmpDir/$dbName;USER=sa;PASSWORD=;CASE_INSENSITIVE_IDENTIFIERS=TRUE;AUTO_SERVER=TRUE;"
    // docker run --rm -p 3306:3306 -e MYSQL_ROOT_PASSWORD=secret -e "TZ=$(date +'%Z %z')" mysql
//    val jdbcUrl = s"jdbc:mysql://root:secret@[::]:3306/"
    // docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=secret postgres
//    val jdbcUrl = s"jdbc:postgresql://[::]:5432/?user=postgres&password=secret"

    val args = scala.Array(
      s"spark_io_config__input_paths__0=$tmpDir/$dbName/$tableName",
      s"spark_io_config__output_path=$tmpDir/output",
      s"db_loader_config__table_names__0=$tableName",
      s"db_loader_config__case_class_names__0=$caseClassName",
      s"db_loader_config__jdbc_url=$jdbcUrl",
      s"db_loader_config__after_load_script=after_load_script.sql",
      s"db_loader_config__executed_sql_output_file=$tmpDir/executed_sql.sql"
    )
    DbLoader.main(scala.Array("--environments=dbloader") ++ args)

    val connection = DriverManager.getConnection(jdbcUrl)
    val latestSchema = getLatestDataSchemaName(connection)
//    println(s">>>> latestSchema = $latestSchema")
//    printQueryResult(jdbcUrl, s"SELECT * FROM information_schema.columns WHERE table_name ILIKE '$tableName'")
//    printQueryResult(jdbcUrl, s"SELECT * FROM information_schema.indexes WHERE table_name ILIKE '$tableName'")
    assert(readItemsFromDb(connection, s"$latestSchema.$tableName", jdbcUrl.startsWith("jdbc:h2")) === items)
    assert(readIndexNamesFromDb(connection, latestSchema, tableName).map(_.toLowerCase).contains("item_by_name"))
  }

  private def printQueryResult(jdbcUrl: String, query: String): Unit = {
    sparkSession.read.format("jdbc")
        .option(JDBCOptions.JDBC_URL, jdbcUrl).option(JDBCOptions.JDBC_QUERY_STRING, query).load().show()
  }

  private def getLatestDataSchemaName(connection: Connection): String = {
    //    val schemaNames = mapResultSet(connection.getMetaData.getSchemas)(_.getString("SCHEMA_NAME"))
    val schemaNames =
      mapQueryResultSet(connection, "SELECT * FROM information_schema.schemata")(_.getString("SCHEMA_NAME"))
    schemaNames.filter(_.matches("(?i)data_.*")).max
  }

  private def readIndexNamesFromDb(connection: Connection, schemaName: String, tableName: String): Seq[String] = {
    val rs = connection.getMetaData.getIndexInfo(null, schemaName, tableName, false, false)
    mapResultSet(rs)(_.getString("INDEX_NAME"))
  }

  private def readItemsFromDb(connection: Connection, tableName: String, unescapeJsonAsString: Boolean): Seq[Item] = {
    mapQueryResultSet(connection, s"SELECT * FROM $tableName") { rs =>
      assert(rs.getMetaData.getColumnCount === 11)
      Item(rs.getInt("id"), rs.getInt("secondary_id"), rs.getString("name"),
        rs.getLong("value_cents"),
        rs.getString("note"), Option(rs.getString("optional_note")),
        rs.getString("long_name_field"),
        rs.getString("custom_type_field"), rs.getDate("date"),
        readJsonValue[Seq[String]](rs, "repeated_field", unescapeJsonAsString),
        readJsonValue[SubItem](rs, "nested_field", unescapeJsonAsString))
    }
  }

  private def mapResultSet[T](resultSetProducer: => ResultSet)(resultSetMapper: ResultSet => T): Seq[T] = {
    IO.using(resultSetProducer) { resultSet =>
      val result = scala.collection.mutable.Buffer.empty[T]
      while (resultSet.next()) {
        result.append(resultSetMapper(resultSet))
      }
      result
    }
  }

  private def mapQueryResultSet[T](connection: Connection, query: String)(resultSetMapper: ResultSet => T): Seq[T] = {
    IO.using(connection.createStatement())(statement => mapResultSet(statement.executeQuery(query))(resultSetMapper))
  }
}

object DbLoaderTest {
  import javax.persistence._
//  import ScalaPersistenceAnnotations._

  case class SubItem(value: String)

  @Entity
  case class Item(
      @Id
      id: Int,
      @Id
      secondaryId: Int,
      @Column(name = "ignored_name", nullable = true, length = 42)
      name: String,
      @Column(nullable = true, precision = 18, scale = 2)
      value_cents: Long,
      @Lob
      note: String,
      @Lob
      optionalNote: Option[String],
      longNameField: String,
      @Column(columnDefinition = "CHAR(42)")
      customTypeField: String,
      date: Date,
      repeatedField: Seq[String],
      nestedField: SubItem)

  // Spark JdbcUtils throw exception when H2 (correctly) returns java.sql.Types.OTHER as column type for json.
  // Spark allows to overwrite catalyst types per jdbc dialect and that is what we do at SparkH2DialectWithJsonSupport.
  object SparkH2DialectWithJsonSupport extends JdbcDialect {
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:h2")

    override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
      if (sqlType == Types.OTHER && typeName.equals("JSON")) {
        Option(StringType)
      } else {
        None
      }
    }
  }

  // Spark insert the json as string. H2 does assume that it is intended as json string and escape what we give it.
  // Hence we have to unescape it. Note: JSON behaviour is very quite different in different dbs!
  def readJsonValue[T: Manifest](rs: ResultSet, columnLabel: String, unescapeJsonAsString: Boolean): T = {
    val dbJsonString = rs.getString(columnLabel)
    val jsonString = if (unescapeJsonAsString) {
      StringEscapeUtils.UNESCAPE_JSON.translate(dbJsonString.substring(1, dbJsonString.length - 1))
    } else {
      dbJsonString
    }
    Jackson.objectMapper.readValue[T](jsonString)
  }
}

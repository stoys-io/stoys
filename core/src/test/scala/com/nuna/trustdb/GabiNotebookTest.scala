package com.nuna.trustdb

import java.nio.file.{Files, Paths}

import com.nuna.trustdb.core.SparkTestBase
import com.nuna.trustdb.core.util.{Jackson, Strings}

import scala.collection.JavaConverters._

class GabiNotebookTest extends SparkTestBase {
//  import sparkSession.implicits._

  import GabiNotebookTest._

  // cd /Users/jan/workspace/tmp/pipenv && pipenv install && pipenv run databricks workspace export /Users/gabi@nuna.com/CareSource_Report/2019_07_22_CS_external_report .
  // cd /Users/jan/workspace/tmp/pipenv && pipenv install && pipenv run databricks workspace export /Users/gabi@nuna.com/CareSource_Report/2019_05_06_CS_Report_Setup .
  test("parse Gabi's notebook") {
    val sqlNotebookPath = Paths.get("/Users/jan/workspace/tmp/pipenv/2019_07_22_CS_external_report.sql")
    //    val sqlNotebookText = Files.readString(sqlNotebookPath)
    val sqlNotebookText = Files.readAllLines(sqlNotebookPath).asScala.mkString("\n")
    val cellTexts = sqlNotebookText.split("-- COMMAND ----------").map(_.trim)
    val cells = cellTexts.zipWithIndex.map({ case (text, index) => parseCell(text, index) })
    val sqlCells = cells.filter(_.language == "%sql")
    println(s">>>> cells = ${cells.size} (sqlCells = ${sqlCells.size})")
    val allOutputNames = sqlCells.flatMap(parseSqlSelectStatements).flatMap(_.outputName)
    println(s">>>> allOutputNames = ${allOutputNames.size} (duplicities = ${
      allOutputNames.groupBy(identity).filter(_._2.size > 1).keys.toList
    })")
    println(s">>>> parseSqlSelectStatements(sqlCells(4)) = ${parseSqlSelectStatements(sqlCells(4))}")
    Jackson.objectMapper.writeValue(
      Paths.get("/tmp/nb.json").toFile, sqlCells.flatMap(parseSqlSelectStatements).map(_.copy(text=null)))
  }
}

object GabiNotebookTest {
  case class Cell(language: String, text: String, title: Option[String], index: Int)
  case class SqlSelectStatement(text: String, dependencies: Seq[String], outputName: Option[String], index: Int,
      cellIndex: Int)

  val createOrReplaceTemporaryViewPattern = "(?is)^CREATE\\sOR\\sREPLACE\\sTEMP(?:ORARY)?\\sVIEW\\s(\\w*)\\sAS\\s(.*)$"
      .r("outputName", "text")
  val selectPattern = "(?is)^(SELECT\\s.*)$".r("text")
  val fromPattern = "(?is)FROM\\s(\\w*)".r("tableName")
  val joinPattern = "(?is)JOIN\\s(\\w*)".r("tableName")

  def getPrefixedLines(text: String, prefix: String): Seq[String] = {
    val fullPrefix = s"-- ${prefix} "
    val lines = text.split('\n')
    lines.filter(_.startsWith(fullPrefix)).map(_.substring(fullPrefix.length))
  }

  def parseCell(text: String, index: Int): Cell = {
    val titles = getPrefixedLines(text, "DBTITLE")
    val title = Strings.trim(titles.mkString("\n"))
    val magics = getPrefixedLines(text, "MAGIC")
    if (magics.isEmpty) {
      val cleanText = text.split('\n').filterNot(_.startsWith("-- DBTITLE")).mkString("\n")
      Cell("%sql", cleanText, title, index)
    } else {
      Cell(magics.head, magics.tail.mkString("\n"), title, index)
    }
  }

  def parseSelectDependencies(text: String): Seq[String] = {
    //    assert(text.startsWith("SELECT"))
    val fromTables = fromPattern.findAllMatchIn(text).map(_.group("tableName")).toList
    val joinTables = joinPattern.findAllMatchIn(text).map(_.group("tableName")).toList
    fromTables ++ joinTables
  }

  def parseSqlSelectStatements(cell: Cell): Seq[SqlSelectStatement] = {
    //    assert(cell.language == "%sql")
    val textWithoutComments = cell.text.split('\n').map(_.split("--").head).mkString("\n")
    val statements = textWithoutComments.split(';').flatMap(Strings.trim)
    statements.zipWithIndex.flatMap {
      case (statementText, index) => statementText match {
        case createOrReplaceTemporaryViewPattern(outputName, text) =>
          Some(SqlSelectStatement(text, parseSelectDependencies(text), Option(outputName), index, cell.index))
        case selectPattern(text) =>
          Some(SqlSelectStatement(text, parseSelectDependencies(text), None, index, cell.index))
        case text =>
          println(s">>>> UNSUPPORTED SQL STATEMENT:\n${text}")
          None
      }
    }
  }
}

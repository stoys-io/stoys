package io.stoys.spark.dq

import io.stoys.spark.SToysException
import io.stoys.spark.SqlUtils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical._

import scala.util.matching.Regex

private object DqSql {
  case class DqSqlInfo(
      rules: Seq[DqRule],
      referenced_table_names: Seq[String],
  )

  def parseDqSql(sparkSession: SparkSession, dqSql: String): DqSqlInfo = {
    val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(dqSql)
    assertQueryLogicalPlan(logicalPlan)
    findTopLevelProject(logicalPlan) match {
      case None =>
        throw new SToysException(s"Select statement required (SELECT ... FROM ...)! Not:\n$dqSql")
      case Some(project) =>
        val rules = extractRules(dqSql, project)
        val referencedRelationshipNames = getReferencedRelationshipNames(logicalPlan)
        DqSqlInfo(rules, referencedRelationshipNames)
    }
  }

  private def findTopLevelProject(logicalPlan: LogicalPlan): Option[Project] = {
    logicalPlan.collectFirst {
      case p: Project => p
    }
  }

  private def extractRules(dqSql: String, project: Project): Seq[DqRule] = {
    val sqlCommentsExtractor = new SqlCommentsExtractor(dqSql)
    project.projectList.zipWithIndex.flatMap {
      case (_: UnresolvedStar, 0) => None
      case (ne, 0) => fail(ne.origin, s"The first expression of dq sql has to be '*' or 'table.*', not:\n$ne")
      case (a: Alias, _) =>
        val comment = a.origin.line.flatMap(sqlCommentsExtractor.getLineCommentsBefore)
        Some(DqRule(a.name, a.child.sql, comment, Seq.empty))
      case (ua: UnresolvedAlias, _) => fail(ua.origin, s"Dq rule needs logical name (like 'rule AS rule_name'):\n$ua")
      case (ne, _) => fail(ne.origin, s"Unsupported expression type ${ne.getClass.getName}:\n$ne")
    }
  }

  private class SqlCommentsExtractor(sql: String) {
    import SqlCommentsExtractor._

    private val lines = Option(sql).toSeq.flatMap(_.linesIterator.toSeq)
    private val lineComments = lines.map(l => SQL_LINE_COMMENT_PATTERN.findFirstMatchIn(l).map(_.group("comment")))

    def getLineCommentsBefore(line: Int): Option[String] = {
      if (line <= 1 || line >= lineComments.length) {
        None
      } else {
        (line - 2).to(0).by(-1).map(lineComments).takeWhile(_.isDefined) match {
          case comments if comments.nonEmpty => Some(comments.flatten.reverse.mkString("\n"))
          case _ => None
        }
      }
    }
  }

  private object SqlCommentsExtractor {
    val SQL_LINE_COMMENT_PATTERN: Regex = "^\\s*--\\s*(.*)\\s*$".r("comment")
  }
}

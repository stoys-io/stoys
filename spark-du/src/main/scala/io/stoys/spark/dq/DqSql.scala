package io.stoys.spark.dq

import io.stoys.spark.SToysException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, UnresolvedAlias, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.Origin

import scala.util.matching.Regex

private[dq] object DqSql {
  case class ParsedDqSql(
      rules: Seq[DqRule],
      referencedTableNames: Set[String]
  )

  def parseReferencedColumnNames(sparkSession: SparkSession, expression: String): Seq[String] = {
    sparkSession.sessionState.sqlParser.parseExpression(expression).references.map(_.name).toSeq
  }

  def parseDqSql(sparkSession: SparkSession, dqSql: String): ParsedDqSql = {
    val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(dqSql)
    assertSafeLogicalPlan(logicalPlan)
    val lastProject = findLastProject(logicalPlan)
//    val primaryRelationshipName = lastProject.toSeq.flatMap(getReferencedRelationshipNames).headOption
    val rules = lastProject.map(lp => extractRules(dqSql, lp)).getOrElse(Seq.empty)
    val referencedRelationshipNames = getReferencedRelationshipNames(logicalPlan)
    ParsedDqSql(rules, referencedRelationshipNames.toSet)
  }

  private def assertSafeLogicalPlan(logicalPlan: LogicalPlan): Unit = {
    logicalPlan.collectWithSubqueries {
      case c: Command => fail(c.origin, s"Unsupported logical plan ${c.getClass.getName}:\n$c")
      case s: ParsedStatement => fail(s.origin, s"Unsupported logical plan ${s.getClass.getName}:\n$s")
    }
  }

  private def findLastProject(logicalPlan: LogicalPlan): Option[Project] = {
    val projects = logicalPlan.collect {
      case p: Project => p
    }
    projects.lastOption
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

  private def getReferencedRelationshipNames(logicalPlan: LogicalPlan): Seq[String] = {
    // TODO: collect does not work over With.innerChild
    val namedRelationNames = logicalPlan.collect {
      case nr: NamedRelation => nr.name
    }
    val subqueryAliases = logicalPlan.collect {
      case sa: SubqueryAlias => sa.alias
    }
    namedRelationNames.filterNot(subqueryAliases.toSet)

    // TODO: fix this function for spark 2.4.x and remove this hack
    Seq.empty
  }

  @inline
  private def fail(origin: Origin, message: String): Nothing = {
    val lineFragment = origin.line.map(_.toString).getOrElse("")
    val columnFragment = origin.startPosition.map(_.toString).getOrElse("")
    throw new SToysException(s"Error at line $lineFragment, column $columnFragment: $message")
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

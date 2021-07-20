package io.stoys.spark.dq

import io.stoys.spark.SToysException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, UnresolvedAlias, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical._

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
    val sqlCommentsExtractor = new SqlCommentsExtractor(dqSql)
    val rules = extractRules(logicalPlan, sqlCommentsExtractor)
    val referencedRelationshipNames = getReferencedRelationshipNames(logicalPlan)
    ParsedDqSql(rules, referencedRelationshipNames)
  }

  @scala.annotation.tailrec
  private def extractRules(logicalPlan: LogicalPlan, sqlCommentsExtractor: SqlCommentsExtractor): Seq[DqRule] = {
    // TODO: can we extract primary table alias?
    logicalPlan match {
      case gl: GlobalLimit => extractRules(gl.child, sqlCommentsExtractor)
      case ll: LocalLimit => extractRules(ll.child, sqlCommentsExtractor)
      case project: Project =>
        project.projectList.zipWithIndex.flatMap {
          case (_: UnresolvedStar, 0) => None
          case (ne, 0) =>
            val msg = s"The first expression (at ${ne.origin}) of dq sql has to be '*' or 'table.*', not:\n$ne"
            throw new SToysException(msg)
          case (a: Alias, _) =>
            val comment = a.origin.line.flatMap(sqlCommentsExtractor.getLineCommentsBefore)
            Some(DqRule(a.name, a.child.sql, comment, Seq.empty))
          case (ua: UnresolvedAlias, _) =>
            throw new SToysException(s"Dq rule (at ${ua.origin}) needs logical name (like 'rule AS rule_name'):\n$ua")
          case (ne, _) =>
            throw new SToysException(s"Unsupported expression type ${ne.getClass.getName} (at ${ne.origin}):\n$ne")
        }
      case s: Sort => extractRules(s.child, sqlCommentsExtractor)
      case w: With => extractRules(w.child, sqlCommentsExtractor)
      case lp => throw new SToysException(s"Unsupported logical plan type ${lp.getClass.getName}:\n$lp")
    }
  }

  private def getReferencedRelationshipNames(logicalPlan: LogicalPlan): Set[String] = {
    // TODO: foreach and two mutable sets?
    def getReferencedRelationshipNamesHelper(lp: LogicalPlan): Seq[String] = {
      val names = lp.collect {
        case nr: NamedRelation => Seq(nr.name)
        case w: With => w.cteRelations.flatMap(r => getReferencedRelationshipNamesHelper(r._2))
      }
      names.flatten
    }

    val referencedRelationshipNames = getReferencedRelationshipNamesHelper(logicalPlan)
    val withStatementDefinedRelationshipNames = logicalPlan.collect {
      case w: With => w.cteRelations.map(_._1)
    }
    referencedRelationshipNames.toSet.diff(withStatementDefinedRelationshipNames.flatten.toSet)

    // TODO: fix this function for spark 2.4.x and remove this hack
    Set.empty
  }

  private class SqlCommentsExtractor(sql: String) {
    import SqlCommentsExtractor._

    private val lines = Option(sql).toSeq.flatMap(_.linesIterator.toSeq)
    private val lineComments = lines.map(l => SqlLineCommentPattern.findFirstMatchIn(l).map(_.group("comment")))

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
    val SqlLineCommentPattern: Regex = "^\\s*--\\s*(.*)\\s*$".r("comment")
  }
}

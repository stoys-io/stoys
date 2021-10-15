package io.stoys.spark

import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, ParsedStatement, SubqueryAlias}
import org.apache.spark.sql.catalyst.trees.{Origin, TreeNode}
import org.apache.spark.sql.catalyst.util.quoteIdentifier

object SqlUtils {
  private val UNQUOTE_SAFE_IDENTIFIER_PATTERN = "^([a-zA-Z_][a-zA-Z0-9_]*)$".r("identifier")

  def quoteIfNeeded(part: String): String = {
    part match {
      case UNQUOTE_SAFE_IDENTIFIER_PATTERN(_) => part
      case p => quoteIdentifier(p)
    }
  }

  def getReferencedColumnNames(expression: Expression): Seq[String] = {
    expression.references.map(_.name).toSeq.distinct.sorted
  }

  def getReferencedRelationshipNames(logicalPlan: LogicalPlan): Seq[String] = {
    def rec(tn: TreeNode[_]): Map[String, Set[String]] = {
      tn match {
        case nr: NamedRelation =>
          Map(nr.name -> Set(nr.name))
        case sa: SubqueryAlias =>
          Map(sa.alias -> rec(sa.child).values.flatten.toSet)
        case tn if withClassNames.contains(tn.getClass.getName) =>
          val cteRRNs = tn.innerChildren.flatMap(rec).toMap
          val childrenRNNs = tn.children.asInstanceOf[Seq[TreeNode[_]]].flatMap(rec).toMap
          childrenRNNs.map(kv => kv._1 -> kv._2.flatMap(n => cteRRNs.getOrElse(n, Set(n))))
        case tn =>
          tn.children.asInstanceOf[Seq[TreeNode[_]]].flatMap(rec).toMap
      }
    }
    rec(logicalPlan).values.flatten.toSeq.distinct.sorted
  }

  def assertQueryLogicalPlan(logicalPlan: LogicalPlan): Unit = {
    logicalPlan.collectWithSubqueries {
      case c: Command => fail(c.origin, s"Unsupported logical plan ${c.getClass.getName}:\n$c")
      case s: ParsedStatement => fail(s.origin, s"Unsupported logical plan ${s.getClass.getName}:\n$s")
    }
  }

  private[stoys] def fail(origin: Origin, message: String): Nothing = {
    val lineFragment = origin.line.map(_.toString).getOrElse("")
    val columnFragment = origin.startPosition.map(_.toString).getOrElse("")
    throw new SToysException(s"Error at line $lineFragment, column $columnFragment: $message")
  }

  // Workaround for renaming class "With" (in Spark 3.0 and 3.1) to "UnresolvedWith" (in Spark 3.2).
  private[stoys] val withClassNames = Seq(
    "org.apache.spark.sql.catalyst.plans.logical.With",
    "org.apache.spark.sql.catalyst.plans.logical.UnresolvedWith",
  )
}

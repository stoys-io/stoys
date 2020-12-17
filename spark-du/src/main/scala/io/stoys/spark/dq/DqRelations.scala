package io.stoys.spark.dq

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import scala.collection.mutable

object DqRelations {
  case class DqJoinKey(
      column_names: Seq[String],
      min_rows: Option[Long],
      max_rows: Option[Long],
      not_null: Boolean,
      unique: Boolean
  )

  case class DqJoin(
      left: DqJoinKey,
      right: DqJoinKey
  )

  private case class JoinKeyCounts(key: Seq[String], left_cnt: Long, right_cnt: Long)

  private case class JoinTypeCounts(key: Seq[String], left_cnt: Long, right_cnt: Long,
      inner: Long, left: Long, right: Long, full: Long)

  case class JoinStatistics(
      left_rows: Long, right_rows: Long, left_distinct: Long, right_distinct: Long,
      inner: Long, left: Long, right: Long, full: Long, cross: Long)

  private def computeJoinKeyCounts(leftDs: Dataset[_], rightDs: Dataset[_],
      leftKey: Seq[String], rightKey: Seq[String]): Dataset[JoinKeyCounts] = {
    import leftDs.sparkSession.implicits._
    assert(leftKey.nonEmpty && rightKey.nonEmpty && leftKey.size == rightKey.size)
    val left = leftDs.groupBy(leftKey.map(col): _*).agg(count(lit(1)).as("__cnt__"))
    val right = rightDs.groupBy(rightKey.map(col): _*).agg(count(lit(1)).as("__cnt__"))
    val joinExprs = leftKey.zip(rightKey).map(lr => leftDs(lr._1) <=> rightDs(lr._2)).reduce(_ && _)
    left.join(right, joinExprs, joinType = "FULL").select(
      array(leftKey.zip(rightKey).map(lr => coalesce(leftDs(lr._1), rightDs(lr._2), lit("NULL"))): _*).as("key"),
      coalesce(left("__cnt__"), lit(0)).as("left_cnt"),
      coalesce(right("__cnt__"), lit(0)).as("right_cnt")
    ).as[JoinKeyCounts]
  }

  private def computeJoinTypeCounts(joinKeyCountsDs: Dataset[JoinKeyCounts]): Dataset[JoinTypeCounts] = {
    import joinKeyCountsDs.sparkSession.implicits._
    joinKeyCountsDs.selectExpr(
      "*",
      "left_cnt * right_cnt AS inner",
      "left_cnt * GREATEST(1, right_cnt) AS left",
      "GREATEST(1, left_cnt) * right_cnt AS right",
      "GREATEST(1, left_cnt) * GREATEST(1, right_cnt) AS full"
    ).as[JoinTypeCounts]
  }

  private def computeJoinStatistics(joinKeyCountsDs: Dataset[JoinKeyCounts]): Dataset[JoinStatistics] = {
    import joinKeyCountsDs.sparkSession.implicits._
    joinKeyCountsDs.selectExpr(
      "SUM(left_cnt) AS left_rows",
      "SUM(right_cnt) AS right_rows",
      "COUNT(left_cnt > 0) AS left_distinct",
      "COUNT(right_cnt > 0) AS right_distinct",
      "SUM(left_cnt * right_cnt) AS inner",
      "SUM(left_cnt * GREATEST(1, right_cnt)) AS left",
      "SUM(GREATEST(1, left_cnt) * right_cnt) AS right",
      "SUM(GREATEST(1, left_cnt) * GREATEST(1, right_cnt)) AS full",
      "SUM(left_cnt) * SUM(right_cnt) AS cross"
    ).as[JoinStatistics]
  }

  private def keyCardinalityRule(side: String, minRows: Option[Long], maxRows: Option[Long]): Option[DqRule] = {
    val expr = (minRows, maxRows) match {
      case (None, None) => None
      case (Some(min), None) => Some(s"$side >= $min")
      case (None, Some(max)) => Some(s"$side <= $max")
      case (Some(min), Some(max)) => Some(s"$side BETWEEN $min AND $max")
    }
    expr.map(e => DqRules.namedRule(side, "key_cardinality", e))
  }

  private def keyNotNullRule(side: String): DqRule = {
    DqRules.namedRule(side, "key_not_null", s"NOT(ARRAY_CONTAINS(key, 'NULL') AND ${side}_cnt > 0)")
  }

  private def keyUniqueRule(side: String): DqRule = {
    DqRules.namedRule(side, "key_unique", s"${side}_cnt <= 1")
  }

  private def optionalRule(predicate: Boolean, rule: => DqRule): Option[DqRule] = {
    if (predicate) {
      Some(rule)
    } else {
      None
    }
  }

  private def generateJoinTypeCountsDqRules(join: DqJoin): Seq[DqRule] = {
    val rules = mutable.Buffer.empty[DqRule]
    rules ++= keyCardinalityRule("left", join.left.min_rows, join.left.max_rows)
    rules ++= keyCardinalityRule("right", join.right.min_rows, join.right.max_rows)
    rules ++= optionalRule(join.left.not_null, keyNotNullRule("left"))
    rules ++= optionalRule(join.right.not_null, keyNotNullRule("right"))
    rules ++= optionalRule(join.left.unique, keyUniqueRule("left"))
    rules ++= optionalRule(join.right.unique, keyUniqueRule("right"))
    rules
  }

  def computeJoinStatistics(leftDs: Dataset[_], rightDs: Dataset[_], join: DqJoin): Dataset[JoinStatistics] = {
    val joinKeyCountsDs = computeJoinKeyCounts(leftDs, rightDs, join.left.column_names, join.right.column_names)
    computeJoinStatistics(joinKeyCountsDs)
  }

  def computeJoinDqResult(leftDs: Dataset[_], rightDs: Dataset[_], join: DqJoin,
      metadata: Map[String, String] = Map.empty): Dataset[DqResult] = {
    val joinKeyCountsDs = computeJoinKeyCounts(leftDs, rightDs, join.left.column_names, join.right.column_names)
    val joinTypeCountsDs = computeJoinTypeCounts(joinKeyCountsDs)
    val rules = generateJoinTypeCountsDqRules(join)
    Dq.fromDataset(joinTypeCountsDs).rules(rules).metadata(metadata).computeDqResult()
  }
}

package io.stoys.spark.dq

import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset}

import scala.collection.mutable

class DqJoin private(leftDs: Dataset[_], rightDs: Dataset[_], joinCondition: Column) {
  import DqJoin._

  private val joinKeyColumnNames = getJoinKeyColumnNames(leftDs, rightDs, joinCondition)
  private val joinKeyCountsDs = computeJoinKeyCounts(leftDs, rightDs, joinCondition, joinKeyColumnNames)
  private val joinTypeCountsDs = computeJoinTypeCounts(joinKeyCountsDs)

  private var metadata: Map[String, String] = Map.empty
  private var joinType: DqJoinType = DqJoinType.UNDEFINED

  def metadata(metadata: Map[String, String]): DqJoin = {
    this.metadata ++= metadata
    this
  }

  def joinType(joinType: DqJoinType): DqJoin = {
    this.joinType = joinType
    this
  }

  def computeDqJoinStatistics(): Dataset[DqJoinStatistics] = {
    DqJoin.computeDqJoinStatistics(joinTypeCountsDs)
  }

  def computeDqResult(): Dataset[DqResult] = {
    val rules = generateJoinTypeCountsDqRules(joinType)
    Dq.fromDataset(joinTypeCountsDs).rules(rules).metadata(metadata ++ getJoinMetadata).computeDqResult()
  }

  def computeDqJoinResult(): Dataset[DqJoinResult] = {
    import joinKeyCountsDs.sparkSession.implicits._
    val dqJoinStatistics = computeDqJoinStatistics()
    val dqResult = computeDqResult()
    val dqJoinResult = dqJoinStatistics.crossJoin(dqResult).select(
      struct(dqJoinStatistics("*")).as("dq_join_statistics"),
      struct(dqResult("*")).as("dq_result")
    )
    dqJoinResult.as[DqJoinResult]
  }

  def getJoinMetadata: Map[String, String] = {
    Map(
      "left_key_column_names" -> joinKeyColumnNames.left.mkString(", "),
      "right_key_column_names" -> joinKeyColumnNames.right.mkString(", "),
      "join_type" -> joinType.toString
    )
  }
}

object DqJoin {
  private case class JoinKeyColumnNames(left: Seq[String], right: Seq[String])

  private case class JoinKeyCounts(key: Seq[String], key_contains_null: Boolean, left_rows: Long, right_rows: Long)

  private case class JoinTypeCounts(key: Seq[String], key_contains_null: Boolean, left_rows: Long, right_rows: Long,
      inner: Long, left: Long, right: Long, full: Long)

  def equiJoin(leftDs: Dataset[_], rightDs: Dataset[_],
      leftColumnNames: Seq[String], rightColumnNames: Seq[String]): DqJoin = {
    assert(leftColumnNames.nonEmpty && rightColumnNames.nonEmpty && leftColumnNames.size == rightColumnNames.size)
    val joinCondition = leftColumnNames.zip(rightColumnNames).map(lr => leftDs(lr._1) <=> rightDs(lr._2)).reduce(_ && _)
    new DqJoin(leftDs, rightDs, joinCondition)
  }

  // TODO: Add approximate version
  def expensiveArbitraryJoin(leftDs: Dataset[_], rightDs: Dataset[_], joinCondition: Column): DqJoin = {
    new DqJoin(leftDs, rightDs, joinCondition)
  }

  private def getJoinKeyColumnNames(
      leftDs: Dataset[_], rightDs: Dataset[_], joinCondition: Column): JoinKeyColumnNames = {
    val joinedDs = leftDs.join(rightDs, joinCondition, joinType = "FULL")
    val joinAnalyzed = joinedDs.queryExecution.analyzed.asInstanceOf[Join]
    val joinConditionAttributes = joinAnalyzed.condition.get.references.toSet
    val leftKeyColumnNames = joinAnalyzed.left.output.filter(joinConditionAttributes.contains).map(_.name)
    val rightKeyColumnNames = joinAnalyzed.right.output.filter(joinConditionAttributes.contains).map(_.name)
    JoinKeyColumnNames(leftKeyColumnNames, rightKeyColumnNames)
  }

  private def computeJoinKeyCounts(leftDs: Dataset[_], rightDs: Dataset[_], joinCondition: Column,
      joinKeyColumnNames: JoinKeyColumnNames): Dataset[JoinKeyCounts] = {
    import leftDs.sparkSession.implicits._
    val leftKeys = joinKeyColumnNames.left
    val rightKeys = joinKeyColumnNames.right
    val left = leftDs.groupBy(leftKeys.map(leftDs.apply): _*).agg(count(lit(1)).as("__rows__"))
    val right = rightDs.groupBy(rightKeys.map(rightDs.apply): _*).agg(count(lit(1)).as("__rows__"))
    val keyComponents = leftKeys.zip(rightKeys).map(lr => coalesce(leftDs(lr._1), rightDs(lr._2), lit("__NULL__")))
    val keyComponentsNull = leftKeys.zip(rightKeys).map(lr => isnull(leftDs(lr._1)) && isnull(rightDs(lr._2)))
    left.join(right, joinCondition, joinType = "FULL").select(
      array(keyComponents: _*).as("key"),
      keyComponentsNull.reduce(_ || _).as("key_contains_null"),
      coalesce(left("__rows__"), lit(0)).as("left_rows"),
      coalesce(right("__rows__"), lit(0)).as("right_rows")
    ).as[JoinKeyCounts]
  }

  private def computeJoinTypeCounts(joinKeyCountsDs: Dataset[JoinKeyCounts]): Dataset[JoinTypeCounts] = {
    import joinKeyCountsDs.sparkSession.implicits._
    joinKeyCountsDs.select(
      col("*"),
      (col("left_rows") * col("right_rows")).as("inner"),
      (col("left_rows") * greatest(lit(1), col("right_rows"))).as("left"),
      (greatest(lit(1), col("left_rows")) * col("right_rows")).as("right"),
      (greatest(lit(1), col("left_rows")) * greatest(lit(1), col("right_rows"))).as("full")
    ).as[JoinTypeCounts]
  }

  private def computeDqJoinStatistics(joinTypeCountsDs: Dataset[JoinTypeCounts]): Dataset[DqJoinStatistics] = {
    import joinTypeCountsDs.sparkSession.implicits._
    joinTypeCountsDs.select(
      sum(col("left_rows")).as("left_rows"),
      sum(col("right_rows")).as("right_rows"),
      sum_if(col("key_contains_null"), col("left_rows")).as("left_nulls"),
      sum_if(col("key_contains_null"), col("right_rows")).as("right_nulls"),
      count_if(not(col("key_contains_null")) && col("left_rows") > 0).as("left_distinct"),
      count_if(not(col("key_contains_null")) && col("right_rows") > 0).as("right_distinct"),
      sum(col("inner")).as("inner"),
      sum(col("left")).as("left"),
      sum(col("right")).as("right"),
      sum(col("full")).as("full"),
      (sum(col("left_rows")) * sum(col("right_rows"))).as("cross")
    ).as[DqJoinStatistics]
  }

  private def keyRule(side: String, logicalName: String, expression: String): DqRule = {
    DqRules.namedRule(s"${side}_key", logicalName, expression)
  }

  private def keyMissingRule(side: String): DqRule = {
    keyRule(side, "missing", s"${side}_rows > 0")
  }

  private def keyDroppingRule(side: String): DqRule = {
    keyRule(side, "dropping", s"${side}_rows > 0")
  }

  private def keyMultiplyingRule(side: String): DqRule = {
    keyRule(side, "multiplying", s"${side}_rows <= 1")
  }

  private def generateJoinTypeCountsDqRules(joinType: DqJoinType): Seq[DqRule] = {
    import DqJoinType._
    val rules = mutable.Buffer.empty[DqRule]
    joinType match {
      case UNDEFINED | INNER =>
        rules += keyDroppingRule("left")
        rules += keyDroppingRule("right")
        rules += keyMultiplyingRule("left")
        rules += keyMultiplyingRule("right")
      case LEFT =>
        rules += keyMissingRule("right")
        rules += keyMultiplyingRule("right")
      case RIGHT =>
        rules += keyMissingRule("left")
        rules += keyMultiplyingRule("left")
      case FULL =>
        rules += keyMissingRule("left")
        rules += keyMissingRule("right")
        rules += keyMultiplyingRule("left")
        rules += keyMultiplyingRule("right")
      case CROSS =>
    }
    rules.toSeq
  }

  private def count_if(condition: Column): Column = {
    count(when(condition, lit(1)).otherwise(lit(null)))
  }

  private def sum_if(condition: Column, value: Column): Column = {
    sum(when(condition, value).otherwise(lit(0)))
  }
}

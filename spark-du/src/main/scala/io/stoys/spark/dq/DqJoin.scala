package io.stoys.spark.dq

import io.stoys.scala.Jackson
import io.stoys.spark.TableName
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.aggregate.CountIf
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset}

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

class DqJoin[L: TypeTag, R: TypeTag] private(leftDs: Dataset[L], rightDs: Dataset[R], joinCondition: Column) {
  import DqJoin._

  private val joinKeyAttributes = getJoinKeyAttributes(leftDs, rightDs, joinCondition)
  private val joinKeyCountsDs = computeJoinKeyCounts(leftDs, rightDs, joinCondition, joinKeyAttributes)
  private val joinTypeCountsDs = computeJoinTypeCounts(joinKeyCountsDs)

  private var config: DqConfig = DqConfig.default
  private var metadata: Map[String, String] = Map.empty
  private var joinType: DqJoinType = DqJoinType.UNDEFINED

  def config(config: DqConfig): DqJoin[L, R] = {
    this.config = config
    this
  }

  def metadata(metadata: Map[String, String]): DqJoin[L, R] = {
    this.metadata = metadata
    this
  }

  def joinType(joinType: DqJoinType): DqJoin[L, R] = {
    this.joinType = joinType
    this
  }

  def getDqJoinInfo: DqJoinInfo = {
    DqJoinInfo(
      left_table_name = TableName.of(leftDs).fullTableName(),
      right_table_name = TableName.of(rightDs).fullTableName(),
      left_key_column_names = joinKeyAttributes.left.map(_.name),
      right_key_column_names = joinKeyAttributes.right.map(_.name),
      join_type = joinType.toString,
      join_condition = joinCondition.expr.sql,
    )
  }

  def computeDqJoinStatistics(): Dataset[DqJoinStatistics] = {
    DqJoin.computeDqJoinStatistics(joinTypeCountsDs)
  }

  def computeDqResult(): Dataset[DqResult] = {
    val rules = generateJoinTypeCountsDqRules(joinType)
    Dq.fromDataset(joinTypeCountsDs).config(config).rules(rules).metadata(metadata).computeDqResult()
  }

  def computeDqJoinResult(): Dataset[DqJoinResult] = {
    import joinKeyCountsDs.sparkSession.implicits._
    val key = DigestUtils.sha256Hex(Jackson.json.writeValueAsString(getDqJoinInfo)).take(7)
    val dqJoinInfo = Seq(getDqJoinInfo).toDS()
    val dqJoinStatistics = computeDqJoinStatistics()
    val dqResult = computeDqResult()
    val dqJoinResult = dqJoinInfo.crossJoin(dqJoinStatistics).crossJoin(dqResult).select(
//      substring(sha2(to_json(struct(dqJoinInfo("*"))), 256), 0, 7).as("key"),
      lit(key).as("key"),
      struct(dqJoinInfo("*")).as("dq_join_info"),
      struct(dqJoinStatistics("*")).as("dq_join_statistics"),
      struct(dqResult("*")).as("dq_result"),
    )
    dqJoinResult.as[DqJoinResult]
  }
}

object DqJoin {
  private case class JoinKeyAttributes(
      left: Seq[Attribute],
      right: Seq[Attribute],
  )

  private case class JoinKeyCounts(
      key: Seq[String],
      key_contains_null: Boolean,
      left_rows: Long,
      right_rows: Long,
  )

  private case class JoinTypeCounts(
      key: Seq[String],
      key_contains_null: Boolean,
      left_rows: Long,
      right_rows: Long,
      inner: Long,
      left: Long,
      right: Long,
      full: Long,
  )

  def equiJoin[L: TypeTag, R: TypeTag](leftDs: Dataset[L], rightDs: Dataset[R],
      leftColumnNames: Seq[String], rightColumnNames: Seq[String]): DqJoin[L, R] = {
    assert(leftColumnNames.nonEmpty && rightColumnNames.nonEmpty && leftColumnNames.size == rightColumnNames.size)
    val joinCondition = leftColumnNames.zip(rightColumnNames).map(lr => leftDs(lr._1) <=> rightDs(lr._2)).reduce(_ && _)
    new DqJoin(leftDs, rightDs, joinCondition)
  }

  // TODO: Add approximate version
  def expensiveArbitraryJoin[L: TypeTag, R: TypeTag](leftDs: Dataset[L], rightDs: Dataset[R],
      joinCondition: Column): DqJoin[L, R] = {
    new DqJoin(leftDs, rightDs, joinCondition)
  }

  private def getJoinKeyAttributes(
      leftDs: Dataset[_], rightDs: Dataset[_], joinCondition: Column): JoinKeyAttributes = {
    val joinedDs = leftDs.join(rightDs, joinCondition, joinType = "FULL")
    val joinAnalyzed = joinedDs.queryExecution.analyzed.asInstanceOf[Join]
    val joinConditionAttributes = joinAnalyzed.condition.get.references.toSet
    val leftKeyAttributes = joinAnalyzed.left.output.filter(joinConditionAttributes.contains)
    val rightKeyAttributes = joinAnalyzed.right.output.filter(joinConditionAttributes.contains)
    JoinKeyAttributes(leftKeyAttributes, rightKeyAttributes)
  }

  private def computeJoinKeyCounts(leftDs: Dataset[_], rightDs: Dataset[_], joinCondition: Column,
      joinKeyAttributes: JoinKeyAttributes): Dataset[JoinKeyCounts] = {
    import leftDs.sparkSession.implicits._
    val leftKeyColumns = joinKeyAttributes.left.map(a => new Column(a))
    val rightKeyColumns = joinKeyAttributes.right.map(a => new Column(a))
    val left = leftDs.groupBy(leftKeyColumns: _*).agg(count(lit(1)).as("__rows__"))
    val right = rightDs.groupBy(rightKeyColumns: _*).agg(count(lit(1)).as("__rows__"))
    val keyComponents = leftKeyColumns.zip(rightKeyColumns).map(lr => coalesce(lr._1, lr._2, lit("__NULL__")))
    val keyComponentsNull = leftKeyColumns.zip(rightKeyColumns).map(lr => isnull(lr._1) && isnull(lr._2))
    left.join(right, joinCondition, joinType = "FULL").select(
      array(keyComponents: _*).as("key"),
      keyComponentsNull.reduce(_ || _).as("key_contains_null"),
      coalesce(left("__rows__"), lit(0)).as("left_rows"),
      coalesce(right("__rows__"), lit(0)).as("right_rows"),
    ).as[JoinKeyCounts]
  }

  private def computeJoinTypeCounts(joinKeyCountsDs: Dataset[JoinKeyCounts]): Dataset[JoinTypeCounts] = {
    import joinKeyCountsDs.sparkSession.implicits._
    joinKeyCountsDs.select(
      col("*"),
      (col("left_rows") * col("right_rows")).as("inner"),
      (col("left_rows") * greatest(lit(1), col("right_rows"))).as("left"),
      (greatest(lit(1), col("left_rows")) * col("right_rows")).as("right"),
      (greatest(lit(1), col("left_rows")) * greatest(lit(1), col("right_rows"))).as("full"),
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
      (sum(col("left_rows")) * sum(col("right_rows"))).as("cross"),
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
    new Column(CountIf(condition.expr).toAggregateExpression())
  }

  private def sum_if(condition: Column, value: Column): Column = {
    sum(when(condition, value).otherwise(lit(0)))
  }
}

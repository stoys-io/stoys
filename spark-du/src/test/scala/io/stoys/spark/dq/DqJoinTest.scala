package io.stoys.spark.dq

import io.stoys.spark.test.SparkTestBase
import org.apache.spark.sql.functions._

class DqJoinTest extends SparkTestBase {
  import DqJoinTest._
  import sparkSession.implicits._

  private val orders = Seq(
    Order(1, "customer_1", "item_1", 42),
    Order(2, "customer_2", "item_2", 22),
    Order(3, "customer_1", "item_2", 42),
    Order(4, "customer_missing", null, 42),
    Order(5, null, "item_2", 42)
  )
  private val items = Seq(Item("item_1"), Item("item_2"), Item("item_3"), Item("item_3"))
  private lazy val orderDs = orders.toDS()
  private lazy val itemDs = items.toDS()

  test("computeJoinStatistics") {
    val join = DqJoin.equiJoin(orderDs, itemDs, Seq("item_id"), Seq("id"))
    assert(join.computeDqJoinStatistics().collect().head === DqJoinStatistics(
      left_rows = 5, right_rows = 4, left_nulls = 1, right_nulls = 0, left_distinct = 2, right_distinct = 3,
      inner = 4, left = 5, right = 6, full = 7, cross = 20))
  }

  test("computeDqResult") {
    val join = DqJoin.equiJoin(orderDs, itemDs, Seq("item_id"), Seq("id")).joinType(DqJoinType.LEFT)
    val dqResult = join.computeDqResult().collect().head
    assert(dqResult.columns.map(_.name)
        === Seq("key", "key_contains_null", "left_rows", "right_rows", "inner", "left", "right", "full"))
    assert(dqResult.statistics.table === DqTableStatistic(4, 2))
    assert(dqResult.statistics.rule === Seq(
      DqRuleStatistics("right_key__missing", 1),
      DqRuleStatistics("right_key__multiplying", 1)
    ))
    assert(dqResult.metadata
        === Map("left_key_column_names" -> "item_id", "right_key_column_names" -> "id", "join_type" -> "LEFT"))
  }

  test("computeDqJoinResult") {
    val join = DqJoin.equiJoin(orderDs, itemDs, Seq("item_id"), Seq("id"))
    val dqJoinResult = join.computeDqJoinResult().collect().head
    assert(dqJoinResult.dq_join_statistics.left === 5)
    assert(dqJoinResult.dq_result.rules.size === 4)
  }

  test("expensiveArbitraryJoin") {
    val joinInCode = DqJoin.expensiveArbitraryJoin(orderDs, itemDs, hash(orderDs("item_id"), itemDs("id")) > 0)
    assert(joinInCode.computeDqJoinStatistics().collect().head.cross === 54)

    val joinInString = DqJoin.expensiveArbitraryJoin(orderDs, itemDs.as("item"), expr("hash(item_id, item.id) > 0"))
    assert(joinInString.computeDqJoinStatistics().collect().head.cross === 54)
  }
}

object DqJoinTest {
  case class Order(id: Int, customer_id: String, item_id: String, amount: Int)
  case class Item(id: String)
}

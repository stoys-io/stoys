package io.stoys.spark.dq

import io.stoys.spark.test.SparkTestBase

class DqRelationsTest extends SparkTestBase {
  import DqRelations._
  import DqRelationsTest._
  import sparkSession.implicits._

  test("relations") {
    val orders = Seq(
      Order(1, "customer_1", "item_1", 42),
      Order(2, "customer_2", "item_2", 22),
      Order(3, "customer_1", "item_2", 42),
      Order(4, "customer_missing", null, 42),
      Order(5, null, "item_2", 42)
    )
    val items = Seq(Item("item_1"), Item("item_2"), Item("item_3"), Item("item_3"))

    val orderDs = orders.toDS()
    orderDs.createOrReplaceTempView("order")
    val itemDs = items.toDS()
    itemDs.createOrReplaceTempView("item")

    val join = DqJoin(
      DqJoinKey(Seq("item_id"), min_rows = Some(0), max_rows = None, not_null = false, unique = false),
      DqJoinKey(Seq("id"), min_rows = Some(0), max_rows = None, not_null = true, unique = true)
    )

    val joinStatisticsDs = computeJoinStatistics(orderDs, itemDs, join)
    val joinStatistics = joinStatisticsDs.collect().head
    assert(joinStatistics === JoinStatistics(5, 4, 4, 4, inner = 4, left = 5, right = 6, full = 7, cross = 20))

    val dqResult = computeJoinDqResult(orderDs, itemDs, join).collect().head
    assert(dqResult.columns.map(_.name) === Seq("key", "left_cnt", "right_cnt", "inner", "left", "right", "full"))
    assert(dqResult.statistics.table === DqTableStatistic(4, 1))
    val expectedRuleStatistics = Seq(
      DqRuleStatistics("left__key_cardinality", 0),
      DqRuleStatistics("right__key_cardinality", 0),
      DqRuleStatistics("right__key_not_null", 0),
      DqRuleStatistics("right__key_unique", 1)
    )
    assert(dqResult.statistics.rule === expectedRuleStatistics)
  }
}

object DqRelationsTest {
  case class Order(id: Int, customer_id: String, item_id: String, amount: Int)
  case class Item(id: String)
}

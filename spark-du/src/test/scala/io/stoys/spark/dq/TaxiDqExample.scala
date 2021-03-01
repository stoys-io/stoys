package io.stoys.spark.dq

import io.stoys.spark.test.SparkTestBase
import io.stoys.spark.test.datasets.TaxiDataset

@org.scalatest.DoNotDiscover
class TaxiDqExample extends SparkTestBase {
  private lazy val taxiDataset = new TaxiDataset(sparkSession)

  test("taxi_dq") {
    val tripDataPlusDf = taxiDataset.readCachedYellowTripDataPlusCsv("yellow_tripdata_2020-02.csv")
    tripDataPlusDf.createOrReplaceTempView("trip_data_plus")

    val dqSql =
      s"""
         |SELECT
         |  *,
         |  passenger_count < 10 AS taxi_is_not_a_bus,
         |  trip_distance > 0.0 AS taxi_is_for_transportation,
         |  total_amount > 0.0 AS taxi_is_not_a_charity,
         |  tpep_pickup_datetime < tpep_dropoff_datetime AS pickup_before_dropoff,
         |  tpep_pickup_datetime BETWEEN TO_DATE("2020-02-01") AND TO_DATE("2020-02-29") AS pickup_in_february_2020,
         |  -- monkeys are cute, smart but annoying :(
         |  NOT ARRAY_CONTAINS(passengers, "🐵") AS no_monkeys_please,
         |  -- everybody loves penguins!
         |  -- have you seen one today?
         |  ARRAY_CONTAINS(passengers, "🐧") AS penguin_is_a_must,
         |  ${1.until(100).map(i => s"total_amount < ${100 * i} AS amount__less_than_${100 * i}").mkString(",\n  ")}
         |FROM
         |  trip_data_plus
         |--LIMIT 42
         |""".stripMargin.trim

    val dq = Dq.fromDqSql(sparkSession, dqSql)
    val dqResult = dq.computeDqResult().collect().head
    val dqResultJsonPath = writeValueAsJsonFile("dq_result.json", dqResult)
    assert(dqResultJsonPath.toFile.exists())
  }
}

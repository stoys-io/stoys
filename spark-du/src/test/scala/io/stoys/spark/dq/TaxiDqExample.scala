package io.stoys.spark.dq

import io.stoys.spark.test.SparkExampleBase
import io.stoys.spark.test.datasets.TaxiDataset

class TaxiDqExample extends SparkExampleBase {
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
         |  total_amount < 100 AS total_amount_is_not_expensive,
         |  total_amount < 1000 AS total_amount_is_not_very_expensive,
         |  tpep_pickup_datetime < tpep_dropoff_datetime AS pickup_before_dropoff,
         |  tpep_pickup_datetime BETWEEN TO_DATE("2020-02-01") AND TO_DATE("2020-02-29") AS pickup_in_february_2020,
         |  -- monkeys are cute, smart but annoying :(
         |  NOT passengers LIKE "%🐵%" AS no_monkeys_please,
         |  -- everybody loves penguins!
         |  -- have you seen one today?
         |  passengers LIKE "*%🐧%" AS penguin_is_a_must
         |FROM
         |  trip_data_plus
         |--LIMIT 42
         |""".stripMargin.trim

    val dq = Dq.fromDqSql(sparkSession, dqSql)
    val dqResult = dq.computeDqResult().collect().head
    val dqResultJsonPath = writeValueAsJsonTmpFile("dq_result.json", dqResult, logFullContent = true)
    assert(dqResultJsonPath.toFile.exists())
  }
}

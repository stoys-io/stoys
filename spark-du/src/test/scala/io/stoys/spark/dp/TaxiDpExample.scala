package io.stoys.spark.dp

import io.stoys.spark.test.SparkTestBase
import io.stoys.spark.test.datasets.TaxiDataset
import io.stoys.spark.{Reshape, ReshapeConfig}

@org.scalatest.DoNotDiscover
class TaxiDpExample extends SparkTestBase {
  private lazy val taxiDataset = new TaxiDataset(sparkSession)

  test("taxi_dp") {
    val tripDataPlusDf = taxiDataset.readCachedYellowTripDataPlusCsv("yellow_tripdata_2020-02.csv")
    // TODO: add decent type inference and remove the reshape call
    val reshapeConfig = ReshapeConfig.dangerous.copy(dropExtraColumns = false)
    val tripDataPlusReshapedDf = Reshape.reshape[TaxiDataset.TripDataPlus](tripDataPlusDf, reshapeConfig)

    val dp = Dp.fromDataset(tripDataPlusReshapedDf)
    val dpResult = dp.computeDpResult().collect().head
    val dpResultJsonPath = writeValueAsJsonFile("dp_result.json", dpResult)
    assert(dpResultJsonPath.toFile.exists())
  }
}

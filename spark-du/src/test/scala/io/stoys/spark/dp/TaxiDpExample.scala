package io.stoys.spark.dp

import io.stoys.spark.test.SparkTestBase
import io.stoys.spark.test.datasets.TaxiDataset

@org.scalatest.DoNotDiscover
class TaxiDpExample extends SparkTestBase {
  private lazy val taxiDataset = new TaxiDataset(sparkSession)

  test("taxi_dp") {
    val tripDataPlusDf = taxiDataset.readCachedYellowTripDataPlusCsv("yellow_tripdata_2020-02.csv")

    val config = DpConfig.default.copy(infer_types_from_strings = true)
    val dp = Dp.fromDataset(tripDataPlusDf).config(config)
    val dpResult = dp.computeDpResult().collect().head
    val dpResultJsonPath = writeValueAsJsonFile("dp_result.json", dpResult)
    assert(dpResultJsonPath.toFile.exists())
  }
}

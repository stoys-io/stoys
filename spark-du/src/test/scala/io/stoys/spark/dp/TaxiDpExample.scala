package io.stoys.spark.dp

import io.stoys.spark.Reshape
import io.stoys.spark.test.SparkExampleBase
import io.stoys.spark.test.datasets.TaxiDataset
import org.apache.spark.sql.types.DoubleType

class TaxiDpExample extends SparkExampleBase {
  private lazy val taxiDataset = new TaxiDataset(sparkSession)

  test("taxi_dp") {
    val tripDataPlusDf = taxiDataset.readCachedYellowTripDataPlusCsv("yellow_tripdata_2020-02.csv")

    // Note: You probably don't want to decrease the items and pmf_buckets! Higher numbers are better when viewed in ui.
    val config = DpConfig.default.copy(items = 4, pmf_buckets = 4, infer_types_from_strings = true)
    val dp = Dp.fromDataset(tripDataPlusDf).config(config)
    val dpResult = dp.computeDpResult().collect().head
    val dpResultJsonPath = writeValueAsJsonTmpFile("dp_result.json", dpResult, logFullContent = true)
    assert(dpResultJsonPath.toFile.exists())

    val targetSchema = DpSchema.updateSchema(tripDataPlusDf.schema, dpResult)
    val reshapedTripDataPlusDf = Reshape.reshapeToDF(tripDataPlusDf, targetSchema)
    assert(reshapedTripDataPlusDf.schema.fields.filter(_.name == "trip_distance").map(_.dataType) === Seq(DoubleType))
    tripDataPlusDf.show(4)
    reshapedTripDataPlusDf.show(4)
  }
}

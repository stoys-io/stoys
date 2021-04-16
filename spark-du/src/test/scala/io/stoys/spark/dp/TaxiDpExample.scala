package io.stoys.spark.dp

import io.stoys.spark.Reshape
import io.stoys.spark.test.SparkTestBase
import io.stoys.spark.test.datasets.TaxiDataset
import org.apache.spark.sql.types.{DoubleType, StringType}

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

    val targetSchema = DpSchema.updateSchema(tripDataPlusDf.schema, dpResult)
    val reshapedTripDataPlusDf = Reshape.reshapeToDF(tripDataPlusDf, targetSchema)
    assert(tripDataPlusDf.schema.fields.filter(_.name == "trip_distance").map(_.dataType) === Seq(StringType))
    assert(reshapedTripDataPlusDf.schema.fields.filter(_.name == "trip_distance").map(_.dataType) === Seq(DoubleType))
  }
}

package io.stoys.spark.dp

import io.stoys.spark.dp.legacy.DpLegacy
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class Dp[T] private(ds: Dataset[T]) {
  private var config: DpConfig = DpConfig.default

  def config(config: DpConfig): Dp[T] = {
    this.config = config
    this
  }

  @deprecated
  def computeDpResultLegacy(): Dataset[DpResult] = {
    DpLegacy.computeDpResult(ds, config)
  }

  def computeDpResult(): Dataset[DpResult] = {
    val aggregator = new DpAggregator(config)
    ds.toDF().select(aggregator.toColumn)
  }
}

object Dp {
  private[dp] val DEFAULT_ZONE_ID = "UTC"

  def fromDataFrame(df: DataFrame): Dp[Row] = {
    fromDataset(df)
  }

  def fromDataset[T](ds: Dataset[T]): Dp[T] = {
    new Dp(ds)
  }
}

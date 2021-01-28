package io.stoys.spark.dp

import org.apache.spark.sql.Dataset

class Dp[T] private(ds: Dataset[T]) {
  private var config: DpConfig = DpConfig.default

  def config(config: DpConfig): Dp[T] = {
    this.config = config
    this
  }

  def computeDpResult(): Dataset[DpResult] = {
    DpFramework.computeDpResult(ds, config)
  }
}

object Dp {
  def fromDataset[T](ds: Dataset[T]): Dp[T] = {
    new Dp(ds)
  }
}

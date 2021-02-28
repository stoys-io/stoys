package io.stoys.spark.dq

import io.stoys.spark.test.SparkTestBase
import io.stoys.spark.test.datasets.Covid19Dataset

@org.scalatest.DoNotDiscover
class Covid19DqExample extends SparkTestBase {
  private lazy val covid19Dataset = new Covid19Dataset(sparkSession)

  private lazy val epidemiologyDf = covid19Dataset.readCachedCovid19Csv("epidemiology.csv")
  private lazy val demographicsDf = covid19Dataset.readCachedCovid19Csv("demographics.csv")

  test("covid19_dq") {
    val dq = Dq.fromDataset(epidemiologyDf).config(DqConfig.default).fields(Seq.empty).rules(Seq.empty)
    val dqResult = dq.computeDqResult().collect().head
    val dqResultJsonPath = writeValueAsJsonFile("dq_result.json", dqResult)
    assert(dqResultJsonPath.toFile.exists())
  }

  test("covid19_dq_join") {
    val dqJoin = DqJoin.equiJoin(epidemiologyDf, demographicsDf, Seq("key"), Seq("key"))
    val dqJoinResult = dqJoin.computeDqJoinResult().collect().head
    val dqJoinResultJsonPath = writeValueAsJsonFile("dq_join_result.json", dqJoinResult)
    assert(dqJoinResultJsonPath.toFile.exists())
  }
}

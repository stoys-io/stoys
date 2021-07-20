package io.stoys.spark.dq

import io.stoys.spark.test.SparkExampleBase
import io.stoys.spark.test.datasets.Covid19Dataset

class Covid19DqExample extends SparkExampleBase {
  private lazy val covid19Dataset = new Covid19Dataset(sparkSession)

  private lazy val epidemiologyDf = covid19Dataset.readCachedCovid19Csv("epidemiology.csv")
  private lazy val demographicsDf = covid19Dataset.readCachedCovid19Csv("demographics.csv")

  test("covid19_dq") {
    val dq = Dq.fromDataset(epidemiologyDf).config(DqConfig.default).fields(Seq.empty).rules(Seq.empty)
    val dqResult = dq.computeDqResult().first()
    val dqResultJsonPath = writeValueAsJsonTmpFile("dq_result.json", dqResult, logFullContent = true)
    assert(dqResultJsonPath.toFile.exists())
  }

  test("covid19_dq_join") {
    val dqJoin = DqJoin.equiJoin(epidemiologyDf, demographicsDf, Seq("key"), Seq("key"))
    val dqJoinResult = dqJoin.computeDqJoinResult().first()
    val dqJoinResultJsonPath = writeValueAsJsonTmpFile("dq_join_result.json", dqJoinResult, logFullContent = true)
    assert(dqJoinResultJsonPath.toFile.exists())
  }
}

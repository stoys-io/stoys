package io.stoys.spark.dp

import java.sql.Date
import io.stoys.spark.test.SparkTestBase
import org.scalactic.Tolerance.convertNumericToPlusOrMinusWrapper

class DpTest extends SparkTestBase {
  import DpTest._
  import io.stoys.spark.test.implicits._
  import sparkSession.implicits._

  test("computeDpResult") {
    val records = Seq(
      Record(false, 1, "foo", 0.0f, "2020-02-20", Array(1), NestedRecord("foo")),
      Record(false, 2, "bar", 42.0f, "2020-01-10", Array(2, 3), NestedRecord("bar")),
      Record(false, 3, "foo", Float.NaN, "2020-03-30", Array.empty, NestedRecord("foo")),
      Record(true, 4, "", Float.NaN, "2020-02-20", Array.empty, NestedRecord(null)),
      Record(null, 0, null, null, null, null, null)
    )

    val config = DpConfig.default.copy(buckets = 4, items = 2)
    val dp = Dp.fromDataset(records.toDS()).config(config)
    val dpResult = dp.computeDpResult().collect().head
    assert(dpResult.table === DpTable(5))
    assert(dpResult.columns.map(_.name) === Seq("b", "i", "s", "f", "dt", "a", "n", "n.value"))
    assert(dpResult.columns.filter(_.name == "f").head === DpColumn(name = "f", data_type = "float",
      count = 5L, count_empty = 2L, count_nulls = 1L, count_unique = 3L, count_zeros = 1L,
      max_length = 4L, min = "0.00", max = "42.00", mean = "21.00",
      histogram = Seq.empty, items = Seq(DpItemCount("NaN", 2L), DpItemCount("0.0", 1L))
    ))
  }

  test("histogram") {
    val config = DpConfig.default.copy(buckets = 4, items = 2)
    val dp = Dp.fromDataset(sparkSession.range(1000)).config(config)
    val dpResult = dp.computeDpResult().collect().head
    val histogram = dpResult.columns.head.histogram
    assert(histogram.size === 4)
    histogram.zipWithIndex.map {
      case (bucketCount, i) =>
          assert(bucketCount.low === 250.0f*i +- 5.0f, s"(histogram($i).low)")
          assert(bucketCount.high === 250.0f*(i+1) +- 5.0f, s"(histogram($i).high)")
          assert(bucketCount.count === 250L +- 5L, s"(histogram($i).count)")
    }
  }
}

object DpTest {
  case class NestedRecord(value: String)
  case class Record(b: Option[Boolean], i: Int, s: String, f: Option[Float], dt: Date, a: Array[Int], n: NestedRecord)
}

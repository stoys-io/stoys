package io.stoys.spark.dp

import io.stoys.scala.Arbitrary
import io.stoys.spark.test.SparkTestBase
import org.scalactic.Tolerance.convertNumericToPlusOrMinusWrapper

import java.sql.Date

class DpTest extends SparkTestBase {
  import DpTest._
  import io.stoys.spark.test.implicits._
  import sparkSession.implicits._

  private val emptyDpColumn = Arbitrary.empty[DpColumn]

  test("computeDpResult - TypedRecord") {
    val typedRecords = Seq(
      TypedRecord(false, 1, "foo", -0.0f, "2020-02-20"),
      TypedRecord(true, 2, "bar", -.0e1f, "2020-02-19"),
      TypedRecord(false, 3, "foo", 42.0f, "2020-02-21"),
      TypedRecord(true, 4, "", Float.NaN, "2020-02-20"),
      TypedRecord(null, 5, null, null, null)
    )

    val config = DpConfig.default.copy(pmf_buckets = 4)
    val dp = Dp.fromDataset(typedRecords.toDS()).config(config)
    val dpResult = dp.computeDpResult().collect().head
    assert(dpResult.table === DpTable(5))
    assert(dpResult.columns.map(_.name) === Seq("b", "i", "s", "f", "dt"))
    val fColumn = dpResult.columns.find(_.name == "f").get
    assert(fColumn.copy(mean = None, pmf = Seq.empty) === DpColumn(
      name = "f",
      data_type = "float",
      nullable = true,
      enum_values = Seq.empty,
      format = None,
      count = 5L,
      count_empty = 1L,
      count_nulls = 1L,
      count_unique = 2L,
      count_zeros = 2L,
      max_length = 4L,
      min = "-0.0",
      max = "42.0",
      mean = None,
      pmf = Seq.empty,
      items = Seq(DpItem("-0.0", 2L), DpItem("42.0", 1L), DpItem("NaN", 1L)),
      extras = Map("quantiles" -> "[-0.0,-0.0,42.0]")
    ))
    assert(fColumn.mean.get === 14.0 +- 0.001)
  }

  test("computeDpResult - StringRecord") {
    val stringRecords = Seq(
      StringRecord("no ", "1", "foo", "-0.0", "2020 🐧 02?20"),
      StringRecord("YES", "2", "bar", "-.0e1", "2020 🐧 02?19"),
      StringRecord(" no", "3", "foo", "42.0", "2020 🐧 02?21"),
      StringRecord("yes", "4", "", "NaN", "2020 🐧 02?20"),
      StringRecord("", "5", null, null, "")
    )

    val typeInferenceConfig = DpTypeInferenceConfig.default.copy(
      prefer_float = true,
      prefer_integer = true,
      prefer_not_nullable = true,
      date_formats = DpTypeInferenceConfig.default.date_formats :+ "yyyy 🐧 MM?dd"
    )
    val config = DpConfig.default.copy(
      pmf_buckets = 2,
      infer_types_from_strings = true,
      type_inference_config = typeInferenceConfig
    )
    val dp = Dp.fromDataset(stringRecords.toDS()).config(config)
    val dpResult = dp.computeDpResult().collect().head
    assert(dpResult.columns.map(DpColumnSimplified.apply) === Seq(
      DpColumnSimplified("b", "boolean", nullable = false).copy(enum_values = Seq("NO", "YES")),
      DpColumnSimplified("i", "integer", nullable = false),
      DpColumnSimplified("s", "string", nullable = true),
      DpColumnSimplified("f", "float", nullable = true),
      DpColumnSimplified("dt", "date", nullable = false).copy(format = Some("yyyy 🐧 MM?dd"))
    ))
    val bColumn = dpResult.columns.find(_.name == "b").get
    assert(bColumn.count_zeros === Some(2))
    val fColumn = dpResult.columns.find(_.name == "f").get
    assert(fColumn.mean.get === 14.0 +- 0.001)
    val dtColumn = dpResult.columns.find(_.name == "dt").get
    assert(dtColumn.items
        === Seq(DpItem("", 1), DpItem("1582070400", 1), DpItem("1582156800", 2), DpItem("1582243200", 1)))
  }

  test("computeDpResult - CollectionRecord") {
    val collectionRecords = Seq(
      CollectionRecord(Array(1), Map("foo" -> 1),  NestedRecord("foo")),
      CollectionRecord(Array(2, 3), Map("bar" -> 2), NestedRecord("bar")),
      CollectionRecord(Array.empty, Map("foo" -> 3, "bar" -> 3), NestedRecord("foo")),
      CollectionRecord(Array.empty, Map.empty, NestedRecord(null)),
      CollectionRecord(null, null, null)
    )

    val dp = Dp.fromDataset(collectionRecords.toDS())
    val dpResult = dp.computeDpResult().collect().head
    assert(dpResult.table === DpTable(5))
    assert(dpResult.columns.map(_.name) === Seq("a", "m", "n"))
    val aColumn = dpResult.columns.find(_.name == "a").get
    assert(aColumn === emptyDpColumn.copy(
      name = "a",
      nullable = true,
      count = 5L,
      count_empty = 2L,
      count_nulls = 1L,
      max_length = 2L
    ))
    val mColumn = dpResult.columns.find(_.name == "m").get
    assert(mColumn === emptyDpColumn.copy(
      name = "m",
      nullable = true,
      count = 5L,
      count_empty = 1L,
      count_nulls = 1L,
      max_length = 2L
    ))
    val nColumn = dpResult.columns.find(_.name == "n").get
    assert(nColumn === emptyDpColumn.copy(
      name = "n",
      nullable = true,
      count = 5L,
      count_nulls = 1L
    ))
  }

  test("pmf") {
    val config = DpConfig.default.copy(pmf_buckets = 4, items = 2)
    val dp = Dp.fromDataset(sparkSession.range(1000)).config(config)
    val dpResult = dp.computeDpResult().collect().head
    val pmf = dpResult.columns.head.pmf
    assert(pmf.size === 4)
    pmf.zipWithIndex.map {
      case (pmfBucket, i) =>
        assert(pmfBucket.low === 250.0 * i +- 10.0, s"(pmf($i).low)")
        assert(pmfBucket.high === 250.0 * (i + 1) +- 10.0, s"(pmf($i).high)")
        assert(pmfBucket.count === 250L +- 10L, s"(pmf($i).count)")
    }
  }
}

object DpTest {
  case class TypedRecord(b: Option[Boolean], i: Int, s: String, f: Option[Float], dt: Date)
  case class StringRecord(b: String, i: String, s: String, f: String, dt: String)
  case class NestedRecord(value: String)
  case class CollectionRecord(a: Array[Int], m: Map[String, Int], n: NestedRecord)

  case class DpColumnSimplified(
      name: String,
      data_type: String,
      nullable: Boolean,
      enum_values: Seq[String],
      format: Option[String]
  )

  object DpColumnSimplified {
    def apply(name: String, dataType: String, nullable: Boolean): DpColumnSimplified = {
      DpColumnSimplified(name, dataType, nullable, Seq.empty, None)
    }

    def apply(dpColumn: DpColumn): DpColumnSimplified = {
      DpColumnSimplified(dpColumn.name, dpColumn.data_type, dpColumn.nullable, dpColumn.enum_values, dpColumn.format)
    }
  }
}

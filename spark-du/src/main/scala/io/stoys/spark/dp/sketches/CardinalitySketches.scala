package io.stoys.spark.dp.sketches

import io.stoys.spark.dp.DpConfig
import org.apache.datasketches.cpc.{CpcSketch, CpcUnion}
import org.apache.spark.sql.Row

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, ZoneId}

private[dp] trait CardinalitySketch {
  def update(value: Any): Unit

  def merge(that: CardinalitySketch): Unit

  def getNumberOfUnique: Option[Long]
}

private[dp] object CardinalitySketch {
  def create(config: DpConfig): CardinalitySketch = {
    DataSketchesCpcSketch.create()
  }
}

private[dp] class DummyCardinalitySketch extends CardinalitySketch {
  override def update(value: Any): Unit = ()

  override def merge(that: CardinalitySketch): Unit = ()

  override def getNumberOfUnique: Option[Long] = None
}

private[dp] class DataSketchesCpcSketch private(
    private var valid: Boolean,
    private var sketch: CpcSketch,
) extends CardinalitySketch {

  override def update(value: Any): Unit = {
    if (valid) {
      value match {
        case null => valid = false
        case string: String => sketch.update(string)
        case number: Number => sketch.update(number.longValue())
//        case decimal: BigDecimal => sketch.update(decimal.doubleValue())
        case binary: Array[Byte] => sketch.update(binary)
        case boolean: Boolean => sketch.update(if (boolean) 1 else 0)
        case date: Date => sketch.update(date.getTime)
        case timestamp: Timestamp => sketch.update(timestamp.getTime)
        case localDate: LocalDate => update(localDate.atStartOfDay(ZoneId.systemDefault()).toEpochSecond)
        case instant: Instant => update(instant.getEpochSecond)
        // TODO: Can we support collections?
        case _: Seq[_] => valid = false
        case _: Map[_, _] => valid = false
        case _: Row => valid = false
      }
    }
  }

  override def merge(that: CardinalitySketch): Unit = {
    that match {
      case that: DataSketchesCpcSketch if this.valid && that.valid =>
        val union = new CpcUnion(this.sketch.getLgK)
        union.update(this.sketch)
        union.update(that.sketch)
        this.sketch = union.getResult
      case _: DataSketchesCpcSketch =>
        this.valid = false
        this.sketch = null
    }
  }

  override def getNumberOfUnique: Option[Long] = {
    if (valid) {
      Some(sketch.getEstimate.toLong)
    } else {
      None
    }
  }
}

private[dp] object DataSketchesCpcSketch {
  def create(): DataSketchesCpcSketch = {
    val lgK = CpcSketch.DEFAULT_LG_K
    new DataSketchesCpcSketch(valid = true, new CpcSketch(lgK))
  }
}

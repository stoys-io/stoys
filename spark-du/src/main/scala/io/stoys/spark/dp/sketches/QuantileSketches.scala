package io.stoys.spark.dp.sketches

import io.stoys.spark.dp.{DpConfig, DpPmfBucket}
import org.apache.datasketches.kll.KllFloatsSketch
import org.apache.datasketches.req.ReqSketch

private[dp] trait QuantileSketch {
  def update(value: Double): Unit

  def merge(that: QuantileSketch): Unit

  def getQuantiles(quantiles: Seq[Double]): Option[Seq[Double]]

  def getPmfBuckets: Seq[DpPmfBucket]
}

private[dp] object QuantileSketch {
  def create(config: DpConfig): QuantileSketch = {
    DataSketchesReqSketch.create(config.pmf_buckets)
  }
}

private[dp] class DummyQuantileSketch extends QuantileSketch {
  override def update(value: Double): Unit = ()

  override def merge(that: QuantileSketch): Unit = ()

  override def getQuantiles(quantiles: Seq[Double]): Option[Seq[Double]] = None

  override def getPmfBuckets: Seq[DpPmfBucket] = Seq.empty
}

private[dp] class DataSketchesKllSketch private(
    private val sketch: KllFloatsSketch,
    private val pmfBuckets: Int
) extends QuantileSketch {

  override def update(value: Double): Unit = {
    sketch.update(value.toFloat)
  }

  override def merge(that: QuantileSketch): Unit = {
    that match {
      case that: DataSketchesKllSketch => this.sketch.merge(that.sketch)
    }
  }

  override def getQuantiles(quantiles: Seq[Double]): Option[Seq[Double]] = {
    if (sketch.isEmpty) {
      None
    } else {
      Some(sketch.getQuantiles(quantiles.toArray).map(_.toDouble).toSeq)
    }
  }

  override def getPmfBuckets: Seq[DpPmfBucket] = {
    pmfBuckets match {
      case _ if sketch.isEmpty => Seq.empty
      case n if n <= 0 => Seq.empty
      case 1 => Seq(DpPmfBucket(sketch.getMinValue, sketch.getMaxValue, sketch.getN))
      case n =>
        val Array(quantileLow, quantileHigh) = sketch.getQuantiles(Array(0.005, 0.995))
        val quantileStep = (quantileHigh - quantileLow) / n
        val low = math.max(sketch.getMinValue, quantileLow - quantileStep / 2.0f)
        val high = math.min(sketch.getMaxValue, quantileHigh + quantileStep / 2.0f)
        val step = (high - low) / n
        val splits = 0.until(n).map(i => low + step * i).distinct.sorted
        if (step.isNaN || step.isInfinite || step == 0.0 || splits.isEmpty) {
          Seq.empty
        } else {
          val pmfValues = sketch.getPMF(splits.drop(1).toArray)
          splits.zip(pmfValues).map(sb => DpPmfBucket(sb._1, sb._1 + step, math.round(sketch.getN * sb._2)))
        }
    }
  }
}

private[dp] object DataSketchesKllSketch {
  def create(pmfBuckets: Int): DataSketchesKllSketch = {
    val k = math.max(KllFloatsSketch.DEFAULT_K, KllFloatsSketch.getKFromEpsilon(1.0 / pmfBuckets, true))
    new DataSketchesKllSketch(new KllFloatsSketch(k), pmfBuckets)
  }
}

private[dp] class DataSketchesReqSketch private(
    private val sketch: ReqSketch,
    private val pmfBuckets: Int
) extends QuantileSketch {

  override def update(value: Double): Unit = {
    sketch.update(value.toFloat)
  }

  override def merge(that: QuantileSketch): Unit = {
    that match {
      case that: DataSketchesReqSketch => this.sketch.merge(that.sketch)
    }
  }

  override def getQuantiles(quantiles: Seq[Double]): Option[Seq[Double]] = {
    if (sketch.isEmpty) {
      None
    } else {
      Some(sketch.getQuantiles(quantiles.toArray).map(_.toDouble).toSeq)
    }
  }

  override def getPmfBuckets: Seq[DpPmfBucket] = {
    pmfBuckets match {
      case _ if sketch.isEmpty => Seq.empty
      case n if n <= 0 => Seq.empty
      case 1 => Seq(DpPmfBucket(sketch.getMinValue, sketch.getMaxValue, sketch.getN))
      case n =>
        val Array(quantileLow, quantileHigh) = sketch.getQuantiles(Array(0.005, 0.995))
        val quantileStep = (quantileHigh - quantileLow) / n
        val low = math.max(sketch.getMinValue, quantileLow - quantileStep / 2.0f)
        val high = math.min(sketch.getMaxValue, quantileHigh + quantileStep / 2.0f)
        val step = (high - low) / n
        val splits = 0.until(n).map(i => low + step * i).distinct.sorted
        if (step.isNaN || step.isInfinite || step == 0.0 || splits.isEmpty) {
          Seq.empty
        } else {
          val pmfValues = sketch.getPMF(splits.drop(1).toArray)
          splits.zip(pmfValues).map(sb => DpPmfBucket(sb._1, sb._1 + step, math.round(sketch.getN * sb._2)))
        }
    }
  }
}

private[dp] object DataSketchesReqSketch {
  def create(pmfBuckets: Int): DataSketchesReqSketch = {
    val defaultK = ReqSketch.builder().getK
    val k = math.max(4, math.min(1024, defaultK + 2 * (math.log(pmfBuckets) / math.log(2) / 2.0).round.toInt))
    val sketch = ReqSketch.builder().setK(k).setHighRankAccuracy(false).build()
    new DataSketchesReqSketch(sketch, pmfBuckets)
  }
}

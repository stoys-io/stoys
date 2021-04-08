package io.stoys.spark.dp.sketches

import io.stoys.spark.dp.{DpConfig, DpPmfBucket}
import org.apache.datasketches.kll.KllFloatsSketch
import org.apache.datasketches.req.ReqSketch

private[dp] trait QuantileSketch {
  def update(value: Double): Unit

  def merge(that: QuantileSketch): Unit

  def getQuantiles(quantiles: Seq[Double]): Seq[Double]

  def getPmfBuckets: Seq[DpPmfBucket]
}

private[dp] object QuantileSketch {
  def create(config: DpConfig): QuantileSketch = {
    DataSketchesReqSketch.create(config.pmf_buckets)
  }
}

private[dp] class DummyQuantileSketch extends QuantileSketch {
  override def update(value: Double): Unit = Unit

  override def merge(that: QuantileSketch): Unit = Unit

  override def getQuantiles(quantiles: Seq[Double]): Seq[Double] = Seq.empty

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

  override def getQuantiles(quantiles: Seq[Double]): Seq[Double] = {
    if (sketch.isEmpty) {
      Seq.empty
    } else {
      sketch.getQuantiles(quantiles.toArray).map(_.toDouble).toSeq
    }
  }

  override def getPmfBuckets: Seq[DpPmfBucket] = {
    if (sketch.isEmpty) {
      Seq.empty
    } else {
      val step = (sketch.getMaxValue - sketch.getMinValue) / pmfBuckets
      val splits = 0.until(pmfBuckets).map(i => sketch.getMinValue + step * i).distinct.sorted
      if (step.isNaN || step.isInfinite || step == 0.0 || splits.isEmpty) {
        Seq.empty
      } else {
        val pmf = sketch.getPMF(splits.drop(1).toArray)
        splits.zip(pmf).map(sb => DpPmfBucket(sb._1, sb._1 + step, math.round(sketch.getN * sb._2)))
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

  override def getQuantiles(quantiles: Seq[Double]): Seq[Double] = {
    if (sketch.isEmpty) {
      Seq.empty
    } else {
      sketch.getQuantiles(quantiles.toArray).map(_.toDouble).toSeq
    }
  }

  override def getPmfBuckets: Seq[DpPmfBucket] = {
    if (sketch.isEmpty) {
      Seq.empty
    } else {
      val step = (sketch.getMaxValue - sketch.getMinValue) / pmfBuckets
      val splits = 0.until(pmfBuckets).map(i => sketch.getMinValue + step * i).distinct.sorted
      if (step.isNaN || step.isInfinite || step == 0.0 || splits.isEmpty) {
        Seq.empty
      } else {
        val pmf = sketch.getPMF(splits.drop(1).toArray)
        splits.zip(pmf).map(sb => DpPmfBucket(sb._1, sb._1 + step, math.round(sketch.getN * sb._2)))
      }
    }
  }
}

private[dp] object DataSketchesReqSketch {
  def create(pmfBuckets: Int): DataSketchesReqSketch = {
    val k = ReqSketch.builder().getK // TODO: use pmfBuckets
    val sketch = ReqSketch.builder().setK(k).setHighRankAccuracy(true).build()
    new DataSketchesReqSketch(sketch, pmfBuckets)
  }
}

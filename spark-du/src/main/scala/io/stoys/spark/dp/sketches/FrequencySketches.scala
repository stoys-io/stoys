package io.stoys.spark.dp.sketches

import io.stoys.spark.dp.{DpConfig, DpItem}
import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}

private[dp] trait FrequencySketch[T] {
  def update(value: T): Unit

  def merge(that: FrequencySketch[T]): Unit

  def getItems(itemToString: T => String): Seq[DpItem]
}

private[dp] object FrequencySketch {
  def create[T](config: DpConfig): FrequencySketch[T] = {
    DataSketchesItemsSketch.create(config.items)
  }
}

private[dp] class DummyFrequencySketch[T] extends FrequencySketch[T] {
  override def update(value: T): Unit = Unit

  override def merge(that: FrequencySketch[T]): Unit = Unit

  override def getItems(itemToString: T => String): Seq[DpItem] = Seq.empty
}

private[dp] class DataSketchesItemsSketch[T] private(
    private var sketch: ItemsSketch[T],
    private val items: Int
) extends FrequencySketch[T] {

  override def update(value: T): Unit = {
    sketch.update(value)
  }

  override def merge(that: FrequencySketch[T]): Unit = {
    that match {
      case that: DataSketchesItemsSketch[T] => this.sketch.merge(that.sketch)
    }
  }

  override def getItems(itemToString: T => String): Seq[DpItem] = {
    val frequentItems = sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES)
    frequentItems.map(i => DpItem(itemToString(i.getItem), i.getEstimate)).toSeq.sortBy(i => -i.count).take(items)
  }
}

private[dp] object DataSketchesItemsSketch {
  private val ItemsSketchLoadFactor = 0.75

  def create[T](items: Int): DataSketchesItemsSketch[T] = {
    val maxMapSize = 1 << math.ceil(math.log(items / ItemsSketchLoadFactor) / math.log(2)).toInt
    new DataSketchesItemsSketch(new ItemsSketch[T](maxMapSize), items)
  }
}

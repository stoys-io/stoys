package io.stoys.spark.dp.sketches

import io.stoys.spark.dp.DpBucketCount
import org.apache.datasketches.kll.KllFloatsSketch
import org.apache.datasketches.memory.Memory
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.types.DataType

private[dp] case class KllFloatsSketchAggregator(child: Expression, buckets: Int,
    mutableAggBufferOffset: Int = 0, inputAggBufferOffset: Int = 0) extends TypedImperativeAggregate[KllFloatsSketch] {
  import KllFloatsSketchAggregator._

  override def createAggregationBuffer(): KllFloatsSketch = {
    val k = math.max(KllFloatsSketch.DEFAULT_K, KllFloatsSketch.getKFromEpsilon(1.0 / oversampling / buckets, true))
    new KllFloatsSketch(k)
  }

  override def update(buffer: KllFloatsSketch, input: InternalRow): KllFloatsSketch = {
    buffer.update(child.eval(input).asInstanceOf[Float])
    buffer
  }

  override def merge(buffer: KllFloatsSketch, input: KllFloatsSketch): KllFloatsSketch = {
    buffer.merge(input)
    buffer
  }

  override def eval(buffer: KllFloatsSketch): Any = {
    val step = (buffer.getMaxValue - buffer.getMinValue) / buckets
    val splits = 0.until(buckets).map(i => buffer.getMinValue + i * step)
    val histogram = if (!splits.sliding(2).forall(s => s(0) < s(1))) {
      Array.empty
    } else {
      // TODO: This should be proper histogram_discrete
      val iterator = buffer.iterator()
      val values = 0.until(buffer.getNumRetained).map { _ =>
        val value = (iterator.getValue, iterator.getWeight)
        iterator.next()
        value
      }
      val itemWeightsMap = values.filter(_._2 > 0).groupBy(_._1).mapValues(_.map(_._2).sum)
      if (itemWeightsMap.size < buffer.getK / 2) {
        // TODO: Do we want some hack to generate histogram even for a discreet dataset?
//        val itemWeights = itemWeightsMap.toSeq.sorted
//        val spacing = if (itemWeights.size == 1) {
//          1.0f
//        } else {
//          itemWeights.sliding(2).map(s => s(1)._1 - s.head._1).min
//        }
//        itemWeights.map(iw => InternalRow(iw._1 - spacing / 2, iw._1 + spacing / 2, iw._2)
        Array.empty
      } else {
        val pmf = buffer.getPMF(splits.drop(1).toArray)
        splits.zip(pmf).map(sc => InternalRow(sc._1, sc._1 + step, (buffer.getN * sc._2).toLong))
      }
    }

    ArrayData.toArrayData(histogram)
  }

  override def serialize(buffer: KllFloatsSketch): Array[Byte] = {
    buffer.toByteArray
  }

  override def deserialize(storageFormat: Array[Byte]): KllFloatsSketch = {
    KllFloatsSketch.heapify(Memory.wrap(storageFormat))
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def nullable: Boolean = false

  override def dataType: DataType = KllFloatsSketchAggregator.dataType

  override def children: Seq[Expression] = Seq(child)
}

private[dp] object KllFloatsSketchAggregator {
  val dataType: DataType = ScalaReflection.schemaFor[Array[DpBucketCount[Float]]].dataType

  private val oversampling = 2.0
}

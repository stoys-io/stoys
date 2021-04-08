package io.stoys.spark.dp.legacy

import io.stoys.spark.dp.DpPmfBucket
import org.apache.datasketches.kll.KllFloatsSketch
import org.apache.datasketches.memory.Memory
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.types.DataType

private[dp] case class DataSketchesKllFloatsSketchAggregator(child: Expression, buckets: Int,
    mutableAggBufferOffset: Int = 0, inputAggBufferOffset: Int = 0) extends TypedImperativeAggregate[KllFloatsSketch] {
  import DataSketchesKllFloatsSketchAggregator._

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
    val splits = 0.until(buckets).map(i => buffer.getMinValue + step * i).distinct.sorted
    val pmf = if (buffer.isEmpty || step.isNaN || step.isInfinite || step == 0.0 || splits.isEmpty) {
      Array.empty
    } else {
      val pmf = buffer.getPMF(splits.drop(1).toArray)
      splits.zip(pmf).map(sb => InternalRow(sb._1.toDouble, (sb._1 + step).toDouble, (buffer.getN * sb._2).toLong))
    }
    ArrayData.toArrayData(pmf)
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

  override def dataType: DataType = DataSketchesKllFloatsSketchAggregator.dataType

  override def children: Seq[Expression] = Seq(child)
}

private[dp] object DataSketchesKllFloatsSketchAggregator {
  val dataType: DataType = ScalaReflection.schemaFor[Array[DpPmfBucket]].dataType

  private val oversampling = 2.0
}

package io.stoys.spark.dp.sketches

import com.twitter.chill.ScalaKryoInstantiator
import io.stoys.spark.dp.DpItem
import org.apache.datasketches.ArrayOfItemsSerDe
import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.datasketches.memory.Memory
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

private[dp] case class ItemsSketchAggregator[T <: AnyRef](child: Expression, items: Int,
    mutableAggBufferOffset: Int = 0, inputAggBufferOffset: Int = 0) extends TypedImperativeAggregate[ItemsSketch[T]] {
  import ItemsSketchAggregator._

  override def createAggregationBuffer(): ItemsSketch[T] = {
    val size = 1 << math.ceil(math.log(oversampling * items / itemsSketchLoadFactor) / math.log(2)).toInt
    new ItemsSketch[T](size)
  }

  override def update(buffer: ItemsSketch[T], input: InternalRow): ItemsSketch[T] = {
    val value = child.eval(input)
    if (value != null) {
      buffer.update(value.asInstanceOf[T])
    }
    buffer
  }

  override def merge(buffer: ItemsSketch[T], input: ItemsSketch[T]): ItemsSketch[T] = {
    buffer.merge(input)
    buffer
  }

  override def eval(buffer: ItemsSketch[T]): Any = {
    val frequentItems = buffer.getFrequentItems(ErrorType.NO_FALSE_POSITIVES)
    val itemCounts = frequentItems.map(fi => UTF8String.fromString(fi.getItem.toString) -> fi.getEstimate)
    val topItemCounts = itemCounts.sortBy(vc => (-vc._2, vc._1)).take(items)
    ArrayData.toArrayData(topItemCounts.map(vc => InternalRow(vc._1, vc._2)))
  }

  override def serialize(buffer: ItemsSketch[T]): Array[Byte] = {
    buffer.toByteArray(new KryoSerDe[T]())
  }

  override def deserialize(storageFormat: Array[Byte]): ItemsSketch[T] = {
    ItemsSketch.getInstance[T](Memory.wrap(storageFormat), new KryoSerDe[T]())
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def nullable: Boolean = false

  override def dataType: DataType = ItemsSketchAggregator.dataType

  override def children: Seq[Expression] = Seq(child)
}

private[dp] object ItemsSketchAggregator {
  val dataType: DataType = ScalaReflection.schemaFor[Array[DpItem]].dataType

  private val itemsSketchLoadFactor = 0.75
  private val oversampling = 2.0

  class KryoSerDe[T <: AnyRef] extends ArrayOfItemsSerDe[T] {
    override def serializeToByteArray(items: Array[T with Object]): Array[Byte] = {
      ScalaKryoInstantiator.defaultPool.toBytesWithClass(items)
    }

    override def deserializeFromMemory(mem: Memory, numItems: Int): Array[T] = {
      val inputBytes = Array.ofDim[Byte](mem.getCapacity.toInt)
      mem.getByteArray(0, inputBytes, 0, inputBytes.length)
      ScalaKryoInstantiator.defaultPool.fromBytes(inputBytes).asInstanceOf[Array[T]]
    }
  }
}

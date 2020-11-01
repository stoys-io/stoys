package io.stoys.spark.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CollectCodes extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Nil)

  override def bufferSchema: StructType = StructType(StructField("codes", ArrayType(StringType)) :: Nil)

  override def dataType: DataType = ArrayType(StringType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq.empty[String]
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getSeq[String](0) ++ input.toSeq.zipWithIndex.flatMap {
      case (codes: Seq[_], _) => codes
      case ("", _) | (null, _) => Seq.empty[String]
      case (code: String, _) => Seq(code)
      case (value, index) => throw new IllegalArgumentException(
        s"CollectCodes: Unsupported argument of type '${value.getClass}' at position $index with value '$value'.")
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getSeq[String](0) ++ buffer2.getSeq[String](0)
  }

  override def evaluate(buffer: Row): Any = {
    CollectCodes.cleanupCodes(buffer.getSeq[String](0))
  }
}

object CollectCodes {
  def cleanupCodes(codes: Seq[String]): Seq[String] = {
    codes.filterNot(s => s == null || s.isEmpty).distinct.sorted
  }
}

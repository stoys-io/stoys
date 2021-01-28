package io.stoys.spark.dp.sketches

import org.apache.spark.sql.Column

private[dp] object functions {
  def kll_floats_sketch(col: Column, buckets: Int): Column = {
//    val aggregateExpression = KllFloatsSketchAggregator(col.expr, buckets).toAggregateExpression()
//    new TypedColumn[Any, Array[DpBucketCount[String]]](aggregateExpression, ExpressionEncoder())
    new Column(KllFloatsSketchAggregator(col.expr, buckets).toAggregateExpression())
  }

  def items_sketch(col: Column, items: Int): Column = {
    new Column(ItemsSketchAggregator(col.expr, items).toAggregateExpression())
  }
}

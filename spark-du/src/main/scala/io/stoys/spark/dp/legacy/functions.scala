package io.stoys.spark.dp.legacy

import org.apache.spark.sql.Column

private[dp] object functions {
  def data_sketches_kll_floats_sketch(col: Column, buckets: Int): Column = {
//    val aggregateExpression = DataSketchesKllFloatsSketchAggregator(col.expr, buckets).toAggregateExpression()
//    new TypedColumn[Any, Array[DpPmfBucket[String]]](aggregateExpression, ExpressionEncoder())
    new Column(DataSketchesKllFloatsSketchAggregator(col.expr, buckets).toAggregateExpression())
  }

  def data_sketches_items_sketch(col: Column, items: Int): Column = {
    new Column(DataSketchesItemsSketchAggregator(col.expr, items).toAggregateExpression())
  }
}

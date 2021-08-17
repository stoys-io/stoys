package io.stoys.spark.dp

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

class DpAggregator(config: DpConfig = DpConfig.default) extends Aggregator[Row, DpAggregator.DpAgg, DpResult] {
  import DpAggregator._

  override def zero: DpAgg = {
    DpAgg(
      count = 0,
      profilers = Array.empty[AnyProfiler]
    )
  }

  override def reduce(agg: DpAgg, row: Row): DpAgg = {
    if (agg.profilers.isEmpty) {
      agg.profilers = row.schema.fields.map(f => AnyProfiler.zero(f, config))
    }
    agg.count += 1
    agg.profilers.indices.foreach { columnIndex =>
      agg.profilers(columnIndex).update(row.get(columnIndex))
    }
    agg
  }

  override def merge(agg1: DpAgg, agg2: DpAgg): DpAgg = {
    if (agg1.count == 0) {
      agg2
    } else {
      agg1.count += agg2.count
      agg1.profilers.zip(agg2.profilers).foreach {
        case (profiler1, profiler2) =>
          profiler1.merge(profiler2)
      }
      agg1
    }
  }

  override def finish(agg: DpAgg): DpResult = {
    DpResult(
      table = DpTable(agg.count),
      columns = agg.profilers.map(_.profile(None, config).orNull)
    )
  }

  override def bufferEncoder: Encoder[DpAgg] = Encoders.kryo[DpAgg]

  override def outputEncoder: Encoder[DpResult] = ExpressionEncoder[DpResult]()
}

object DpAggregator {
  case class DpAgg(
      var count: Long,
      var profilers: Array[AnyProfiler]
  )
}

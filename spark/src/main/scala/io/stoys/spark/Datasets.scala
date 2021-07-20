package io.stoys.spark

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias

import scala.reflect.runtime.universe.TypeTag

object Datasets {
  def getAlias(ds: Dataset[_]): Option[String] = {
    ds.queryExecution.analyzed match {
      case sa: SubqueryAlias => Some(sa.alias)
      case _ => None
    }
  }

  class RichDataset(val ds: Dataset[_]) extends AnyVal {
    def reshape[T <: Product : TypeTag]: Dataset[T] = {
      Reshape.reshape[T](ds)
    }

    def reshape[T <: Product : TypeTag](config: ReshapeConfig): Dataset[T] = {
      Reshape.reshape[T](ds, config)
    }

    def getAlias: Option[String] = {
      Datasets.getAlias(ds)
    }
  }
}

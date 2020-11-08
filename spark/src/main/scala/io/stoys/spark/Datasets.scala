package io.stoys.spark

import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe.TypeTag

object Datasets {
  class RichDataset(val ds: Dataset[_]) extends AnyVal {
    def reshape[T <: Product : TypeTag]: Dataset[T] = {
      Reshape.reshape[T](ds)
    }

    def reshape[T <: Product : TypeTag](config: ReshapeConfig): Dataset[T] = {
      Reshape.reshape[T](ds, config)
    }
  }
}

package com.nuna.trustdb.core.spark

import com.nuna.trustdb.core.util.Reflection
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.runtime.universe.TypeTag

object Datasets {
  /**
   * asDataset[T] is similar to spark's own as[T]
   *
   * But this one actually force the schema change to the underlying DataFrame (using projection - "select" statement).
   * The result is like as[T] but only having the fields specified in case class T and in the order they are in there.
   *
   * @param ds input [[Dataset]][T] or [[DataFrame]]
   * @tparam T case class to which we are casting the df
   * @return [[Dataset]][T] with only the columns present in case class and in that order
   */
  // TODO: Can we make this work for nested records?
  def asDataset[T <: Product : TypeTag](ds: Dataset[_]): Dataset[T] = {
    import ds.sparkSession.implicits._

    val actualColumns = ds.columns.toSeq
    val expectedColumns = Reflection.getCaseClassFieldNames[T]

    if (actualColumns == expectedColumns) {
      ds.as[T]
    } else {
      val missingColumns = expectedColumns.toSet -- actualColumns
      if (missingColumns.nonEmpty) {
        throw new RuntimeException(s"Data frame with columns $actualColumns is missing columns $missingColumns")
      }

      val duplicateColumns = actualColumns.diff(actualColumns.distinct).distinct
      if (duplicateColumns.nonEmpty) {
        throw new RuntimeException(s"Data frame has duplicate columns $duplicateColumns")
      }

      ds.select(expectedColumns.map(col): _*).as[T]
    }
  }

  class RichDataset(val ds: Dataset[_]) extends AnyVal {
    def asDataset[T <: Product : TypeTag]: Dataset[T] = {
      Datasets.asDataset[T](ds)
    }
  }
}

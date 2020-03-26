package com.nuna.trustdb.core.spark

import com.nuna.trustdb.core.util.Reflection
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

import scala.reflect.runtime.universe._

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
  def asDataset[T: Encoder : TypeTag](ds: Dataset[_]): Dataset[T] = {
    // TODO: Can we make this work for nested records?
    val actualColumns = ds.columns.toSeq
    val expectedColumns = Reflection.getCaseClassFields[T].map(_.name.decodedName.toString)

    if (actualColumns == expectedColumns) {
      ds.as[T]
    } else {
      val missingColumns = expectedColumns.toSet -- actualColumns
      if (missingColumns.nonEmpty) {
        throw new RuntimeException(s"Data frame with columns $actualColumns is missing columns: $missingColumns")
      }

      val duplicateColumns = actualColumns.diff(actualColumns.distinct).distinct
      if (duplicateColumns.nonEmpty) {
        throw new RuntimeException(s"Data frame has duplicate columns $duplicateColumns")
      }

      ds.select(expectedColumns.map(col): _*).as[T]
    }
  }

  class RichDataset(val ds: Dataset[_]) extends AnyVal {
    def asDataset[T: Encoder : TypeTag]: Dataset[T] = {
      Datasets.asDataset[T](ds)
    }
  }
}

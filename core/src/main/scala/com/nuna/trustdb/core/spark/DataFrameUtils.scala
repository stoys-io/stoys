package com.nuna.trustdb.core.spark

import com.nuna.trustdb.core.util.Reflection
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

import scala.reflect.runtime.universe._

object DataFrameUtils {
  /**
   * asDataset[T] is similar to spark's own as[T]
   *
   * But this one actually force the schema change to the underlying DataFrame (using projection - "select" statement).
   * The result is like as[T] but only having the fields specified in case class T and in the order they are in there.
   *
   * @param df input DataFrame
   * @tparam T case class to which we are casting the df
   * @return [[Dataset]][T] with only the columns present in case class and in that order
   */
  def asDataset[T: Encoder : TypeTag](df: DataFrame): Dataset[T] = {
    // TODO: Can we make this work for nested records?
    val actualColumns = df.columns.toList
    val expectedColumns = Reflection.getCaseClassFields[T].map(_.name.decodedName.toString)

    if (actualColumns == expectedColumns) {
      df.as[T]
    } else {
      val missingColumns = expectedColumns.toSet -- actualColumns
      if (missingColumns.nonEmpty) {
        throw new RuntimeException(s"Data frame with columns $actualColumns is missing columns: $missingColumns")
      }

      val duplicateColumns = actualColumns.diff(actualColumns.distinct).distinct
      if (duplicateColumns.nonEmpty) {
        throw new RuntimeException(s"Data frame has duplicate columns $duplicateColumns")
      }

      df.select(expectedColumns.map(col): _*).as[T]
    }
  }

  class RichDataFrame(val df: DataFrame) extends AnyVal {
    def asDataset[T: Encoder : TypeTag]: Dataset[T] = {
      DataFrameUtils.asDataset[T](df)
    }
  }
}

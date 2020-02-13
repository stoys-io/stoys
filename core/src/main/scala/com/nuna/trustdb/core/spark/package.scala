package com.nuna.trustdb.core

import org.apache.spark.sql.DataFrame

package object spark {
  object implicits {
    import scala.language.implicitConversions

    implicit def toRichDataFrame(df: DataFrame): DataFrameUtils.RichDataFrame = {
      new DataFrameUtils.RichDataFrame(df)
    }
  }
}

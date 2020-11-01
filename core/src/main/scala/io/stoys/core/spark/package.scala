package io.stoys.core

import org.apache.spark.sql.{Dataset}

package object spark {
  object implicits {
    import scala.language.implicitConversions

    implicit def toRichDataset(ds: Dataset[_]): Datasets.RichDataset = {
      new Datasets.RichDataset(ds)
    }
  }
}

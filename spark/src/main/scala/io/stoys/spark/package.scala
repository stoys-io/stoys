package io.stoys

import org.apache.spark.sql.Dataset

package object spark {
  object implicits {
    import _root_.scala.language.implicitConversions

    implicit def toRichDataset(ds: Dataset[_]): Datasets.RichDataset = {
      new Datasets.RichDataset(ds)
    }
  }
}

package io.stoys.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.DataSourceRegister

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe._

object Spark {
  val rowTypeTag: TypeTag[Row] = typeTag[Row]

  def isDeltaSupported: Boolean = {
    ServiceLoader.load(classOf[DataSourceRegister]).iterator().asScala.exists(_.shortName().toLowerCase() == "delta")
  }
}

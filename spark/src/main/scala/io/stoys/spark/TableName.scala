package io.stoys.spark

import io.stoys.scala.{Reflection, Strings}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.{Dataset, Row}

import scala.reflect.runtime.universe._
import scala.util.Random

case class TableName[T: TypeTag] private[spark](entityName: Option[String], logicalName: Option[String]) {
  import TableName._

  val typeTag: TypeTag[T] = implicitly[TypeTag[T]]

  def fullTableName(): String = {
    val rawFullTableName = (entityName, logicalName) match {
      case (Some(entityName), Some(logicalName)) => s"$entityName$LOGICAL_NAME_SEPARATOR$logicalName"
      case (Some(entityName), None) => entityName
      case (None, Some(logicalName)) => s"$LOGICAL_NAME_SEPARATOR$logicalName"
      case (None, None) => throw new SToysException("Table name has to have at least one of (entityName, logicalName)")
    }
    Strings.toSnakeCase(Strings.toWordCharactersCollapsing(rawFullTableName))
  }
}

object TableName {
  val LOGICAL_NAME_SEPARATOR = "__"

  def apply[T <: Product : TypeTag]: TableName[T] = {
    TableName.apply[T](logicalName = null)
  }

  def apply[T <: Product : TypeTag](logicalName: String): TableName[T] = {
    TableName(Some(Strings.toSnakeCase(Reflection.typeNameOf[T])), Option(logicalName))
  }

  def of[T: TypeTag](ds: Dataset[T]): TableName[T] = {
    Datasets.getAlias(ds).flatMap(parse) match {
      case Some(stn) =>
        TableName[T](stn.entityName, stn.logicalName)
      case None if typeOf[T] =:= typeOf[Row] =>
        TableName[T](None, Option(getDataFrameLogicalName(ds)))
      case None if Reflection.isSubtype(typeOf[T], typeOf[Product]) =>
        TableName[T](Option(Reflection.typeNameOf[T]), None)
    }
  }

  private[spark] case class SimpleTableName(
    entityName: Option[String],
    logicalName: Option[String]
  )

  private[spark] def parse(fullTableName: String): Option[SimpleTableName] = {
    fullTableName.split(LOGICAL_NAME_SEPARATOR, 2) match {
      case Array("", logicalName) => Some(SimpleTableName(None, Strings.trim(logicalName)))
      case Array(entityName) => Some(SimpleTableName(Strings.trim(entityName), None))
      case Array(entityName, logicalName) => Some(SimpleTableName(Strings.trim(entityName), Strings.trim(logicalName)))
      case _ => None
    }
  }

  private def getDataFrameLogicalName(ds: Dataset[_]): String = {
    val schemaHash = DigestUtils.sha256Hex(ds.schema.json)
    // TODO: Should we use ds.queryExecution.id on Spark 3+?
//    s"${schemaHash.take(7)}$LOGICAL_NAME_SEPARATOR${ds.queryExecution.id}"
    s"${schemaHash.take(7)}$LOGICAL_NAME_SEPARATOR${math.abs(Random.nextLong())}"
  }
}

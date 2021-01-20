package io.stoys.spark

import io.stoys.scala.{Reflection, Strings}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

case class TableName[T <: Product : TypeTag] private(entityName: String, logicalName: Option[String]) {
  val typeTag: universe.TypeTag[T] = implicitly[TypeTag[T]]

  def fullTableName(): String = {
    import TableName.LOGICAL_NAME_SEPARATOR
    val rawFullTableName = logicalName.map(ln => s"$entityName$LOGICAL_NAME_SEPARATOR$ln").getOrElse(entityName)
    Strings.toSnakeCase(Strings.toWordCharactersCollapsing(rawFullTableName))
  }
}

object TableName {
  val LOGICAL_NAME_SEPARATOR = "__"

  def apply[T <: Product : TypeTag]: TableName[T] = {
    TableName.apply[T](logicalName = null)
  }

  def apply[T <: Product : TypeTag](logicalName: String): TableName[T] = {
    TableName(Strings.toSnakeCase(Reflection.typeNameOf[T]), Option(logicalName))
  }
}

package io.stoys.spark

import io.stoys.scala.{Reflection, Strings}

import scala.reflect.runtime.universe._

class TableNameLookup(classes: Iterable[Class[_]]) {
  private val logger = org.log4s.getLogger

  private val typeTagByEntityName = classes.flatMap(getEntityNameToTypeTag).toMap

  private def getEntityNameToTypeTag(clazz: Class[_]): Option[(String, TypeTag[Product])] = {
    Reflection.classNameToTypeTag(clazz.getName) match {
      case typeTag if Reflection.isCaseClass(typeTag) =>
        val entityName = Strings.toSnakeCase(clazz.getSimpleName)
        Some(entityName -> typeTag.asInstanceOf[TypeTag[Product]])
      case _ =>
        logger.error(s"Class ${clazz.getName} is not a case class!")
        None
    }
  }

  private def getTableName[T <: Product](entityName: String, logicalName: Option[String]): Option[TableName[T]] = {
    typeTagByEntityName.get(entityName) match {
      case Some(typeTag) => Some(TableName[T](entityName, logicalName)(typeTag.asInstanceOf[TypeTag[T]]))
      case None =>
        logger.error(s"No class registered for entity name $entityName!")
        None
    }
  }

  def parse[T <: Product](fullTableName: String): Option[TableName[T]] = {
    fullTableName.split(TableName.LOGICAL_NAME_SEPARATOR) match {
      case Array(entityName) => getTableName[T](entityName, None)
      case Array(entityName, logicalName) => getTableName[T](entityName, Option(logicalName))
      case _ =>
        logger.error(s"Wrong table name format '$fullTableName'. It should be 'entity_name[__logical_name]'.")
        None
    }
  }
}

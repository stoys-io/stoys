package io.stoys.spark

import io.stoys.scala.{Reflection, Strings}

import scala.reflect.runtime.universe._

class TableNameLookup(classes: Iterable[Class[_]]) {
  private val logger = org.log4s.getLogger

  private val typeTagByEntityName = getTypeTagByEntityName(classes)

  private def getTypeTagByEntityName(classes: Iterable[Class[_]]): Map[String, TypeTag[Product]] = {
    classes.flatMap({ clazz =>
      Reflection.classNameToTypeTag(clazz.getName) match {
        case typeTag if Reflection.isCaseClass(typeTag) =>
          val entityName = Strings.toSnakeCase(clazz.getSimpleName)
          Some(entityName -> typeTag.asInstanceOf[TypeTag[Product]])
        case _ =>
          logger.error(s"Class ${clazz.getName} is not a case class!")
          None
      }
    }).toMap
  }

  private def maybeTableName[T <: Product](entityName: String, logicalName: Option[String]): Option[TableName[T]] = {
    typeTagByEntityName.get(entityName) match {
      case Some(typeTag) => Some(TableName[T](entityName, logicalName)(typeTag.asInstanceOf[TypeTag[T]]))
      case None =>
        logger.error(s"No class registered for entity name $entityName!")
        None
    }
  }

  def parse[T <: Product](fullTableName: String): Option[TableName[T]] = {
    fullTableName.split(TableName.LOGICAL_NAME_SEPARATOR) match {
      case Array(entityName) => maybeTableName[T](entityName, None)
      case Array(entityName, logicalName) => maybeTableName[T](entityName, Option(logicalName))
      case _ =>
        logger.error(s"Wrong table name format '$fullTableName'. It should be 'entity_name[__logical_name]'.")
        None
    }
  }
}

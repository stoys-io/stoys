package io.stoys.spark

import io.stoys.scala.{Reflection, Strings}
import org.apache.spark.sql.Row

import scala.reflect.runtime.universe._

class TableNameLookup(classes: Iterable[Class[_]]) {
  private val logger = org.log4s.getLogger

  private val typeTagByEntityName = classes.flatMap(getEntityNameToTypeTag).toMap

  private def getEntityNameToTypeTag(clazz: Class[_]): Option[(String, TypeTag[_])] = {
    Reflection.classNameToTypeTag(clazz.getName) match {
      case typeTag if Reflection.isCaseClass(typeTag) =>
        val entityName = Strings.toSnakeCase(clazz.getSimpleName)
        Some(entityName -> typeTag)
      case _ =>
        logger.error(s"Class ${clazz.getName} is not a case class!")
        None
    }
  }

  private def lookupEntityTableName[T <: Product](
      entityName: String, logicalName: Option[String]): Option[TableName[T]] = {
    typeTagByEntityName.get(entityName) match {
      case Some(typeTag) =>
        Some(TableName(Option(entityName), logicalName)(typeTag.asInstanceOf[TypeTag[T]]))
      case None =>
        logger.error(s"No class registered for entity name $entityName!")
        None
    }
  }

  def lookupEntityTableName[T <: Product](fullTableName: String): Option[TableName[T]] = {
    TableName.parse(fullTableName) match {
      case Some(stn) if stn.entityName.isDefined => lookupEntityTableName(stn.entityName.get, stn.logicalName)
      case _ =>
        val supportedFormatsMsg = "'entity_name' or 'entity_name__logical_name'"
        logger.error(s"Wrong entity table name format '$fullTableName'. It should be: $supportedFormatsMsg.")
        None
    }
  }

  def lookupTableName(fullTableName: String): Option[TableName[_]] = {
    TableName.parse(fullTableName) match {
      case Some(stn) =>
        stn.entityName match {
          case Some(entityName) => lookupEntityTableName(entityName, stn.logicalName)
          case None => Some(TableName[Row](stn.entityName, stn.logicalName))
        }
      case None =>
        val supportedFormatsMsg = "'entity_name', 'entity_name__logical_name' or '__logical_name'"
        logger.error(s"Wrong table name format '$fullTableName'. It should be: $supportedFormatsMsg.")
        None
    }
  }
}

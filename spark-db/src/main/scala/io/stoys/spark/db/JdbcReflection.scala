package io.stoys.spark.db

import io.stoys.scala.{Reflection, Strings}
import io.stoys.spark.TableName

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

object JdbcReflection {
  val DEFAULT_COLUMN_LENGTH: Int =
    classOf[javax.persistence.Column].getDeclaredMethod("length").getDefaultValue.asInstanceOf[java.lang.Integer]

  def getColumnName(symbol: Symbol): String = {
    Strings.toSnakeCase(Reflection.termNameOf(symbol))
  }

  def getQualifiedTableName[T <: Product](tableName: TableName[T], schemaName: String): String = {
    val fullTableName = tableName.fullTableName()
    Option(schemaName).map(sn => s"$sn.$fullTableName").getOrElse(fullTableName)
  }

  def getCreateTableStatement[T <: Product](tableName: TableName[T], schemaName: String): String = {
    implicit val typeTagT: universe.TypeTag[T] = tableName.typeTag
    Reflection.assertAnnotatedCaseClass[T, javax.persistence.Entity]()
    val qualifiedTableName = getQualifiedTableName[T](tableName, schemaName)
    val sqlTypes = Reflection.getCaseClassFields[T].map(getSqlTypeName)
    sqlTypes.mkString(s"CREATE TABLE $qualifiedTableName (\n  ", ",\n  ", "\n);")
  }

  def getAddConstraintStatements[T <: Product](tableName: TableName[T], schemaName: String): Seq[String] = {
    implicit val typeTagT: universe.TypeTag[T] = tableName.typeTag
    Reflection.assertAnnotatedCaseClass[T, javax.persistence.Entity]()
    val fields = Reflection.getCaseClassFields[T]
    val fullTableName = tableName.fullTableName()
    val qualifiedTableName = getQualifiedTableName[T](tableName, schemaName)
    val fieldsWithColumnAnnotation =
      fields.flatMap(f => Reflection.getAnnotationParams[javax.persistence.Column](f).map(ap => (f, ap.toMap)))
    val uniqueColumnConstraints = fieldsWithColumnAnnotation.collect {
      case (field, columnAnnotationParams) if columnAnnotationParams.getOrElse("unique", false) == true =>
        val columnName = getColumnName(field)
        s"ALTER TABLE $qualifiedTableName ADD CONSTRAINT ${fullTableName}_$columnName UNIQUE ($columnName);"
    }
    val idColumnNames = fields.collect {
      case field if field.annotations.exists(_.tree.tpe =:= typeOf[javax.persistence.Id]) => getColumnName(field)
    }
    val uniqueKeyConstraint = Option(idColumnNames).filter(_.nonEmpty).map(columnNames =>
      s"ALTER TABLE $qualifiedTableName ADD PRIMARY KEY (${columnNames.mkString(", ")});")
    uniqueColumnConstraints ++ uniqueKeyConstraint
  }

  // Note: This may need some work per db type. It seems fine for mysql, postgres and h2.
  def getSqlTypeName(param: Symbol): String = {
    val column = Reflection.getAnnotationParams[javax.persistence.Column](param).getOrElse(Seq.empty).toMap
    val lob = Reflection.getAnnotationParams[javax.persistence.Lob](param)
    val rawParamType = param.typeSignature.dealias
    val isOption = rawParamType <:< typeOf[Option[_]]
    val maybeNotNullSuffix = if (isOption) "" else " NOT NULL"
    val paramType = if (isOption) rawParamType.typeArgs.head else rawParamType
    val fullSqlType = if (column.contains("columnDefinition")) {
      column("columnDefinition")
    } else if (paramType =:= typeOf[String] && lob.isDefined) {
      s"TEXT$maybeNotNullSuffix"
    } else {
      val sqlType = paramType match {
        case t if t =:= typeOf[Boolean] => "BOOLEAN"
//        case t if t =:= typeOf[Byte] => "TINYINT UNSIGNED"
//        case t if t =:= typeOf[Short] => "SMALLINT"
//        case t if t =:= typeOf[Char] => "CHAR"
        case t if t =:= typeOf[Int] => "INT"
        case t if t =:= typeOf[Long] => "BIGINT"
        case t if t =:= typeOf[Float] => "FLOAT"
        case t if t =:= typeOf[Double] => "DOUBLE"
        case t if t =:= typeOf[String] => s"VARCHAR(${column.getOrElse("length", DEFAULT_COLUMN_LENGTH)})"
        case t if t =:= typeOf[java.sql.Date] => "DATE"
        case t if t <:< typeOf[Iterable[_]] => "JSON"
        case t if Reflection.isCaseClass(t) => "JSON"
        case _ =>
          throw new IllegalArgumentException(s"Unsupported type ${Reflection.renderAnnotatedSymbol(param)}!")
      }
      s"$sqlType$maybeNotNullSuffix"
    }
    s"${getColumnName(param)} $fullSqlType"
  }
}

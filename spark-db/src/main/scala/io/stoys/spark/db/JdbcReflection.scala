package io.stoys.spark.db

import io.stoys.scala.{Reflection, Strings}
import io.stoys.spark.{SToysException, TableName}

import scala.reflect.runtime.universe._

object JdbcReflection {
  import Reflection._

  val DEFAULT_COLUMN_LENGTH: Int =
    classOf[javax.persistence.Column].getDeclaredMethod("length").getDefaultValue.asInstanceOf[java.lang.Integer]

  private def getColumnName(symbol: Symbol): String = {
    Strings.toSnakeCase(nameOf(symbol))
  }

  def getQualifiedTableName[T <: Product](tableName: TableName[T], schemaName: String): String = {
    val fullTableName = tableName.fullTableName()
    Option(schemaName).map(sn => s"$sn.$fullTableName").getOrElse(fullTableName)
  }

  def getCreateTableStatement[T <: Product](tableName: TableName[T], schemaName: String): String = {
    assertAnnotatedCaseClass[javax.persistence.Entity](tableName.typeTag.tpe)
    val fields = getCaseClassFields(tableName.typeTag.tpe)
    val qualifiedTableName = getQualifiedTableName[T](tableName, schemaName)
    val sqlTypes = cleanupReflection(fields.map(getSqlTypeName))
    sqlTypes.mkString(s"CREATE TABLE $qualifiedTableName (\n  ", ",\n  ", "\n);")
  }

  def getAddConstraintStatements[T <: Product](tableName: TableName[T], schemaName: String): Seq[String] = {
    assertAnnotatedCaseClass[javax.persistence.Entity](tableName.typeTag.tpe)
    val fields = getCaseClassFields(tableName.typeTag.tpe)
    val fullTableName = tableName.fullTableName()
    val qualifiedTableName = getQualifiedTableName[T](tableName, schemaName)
    val fieldsWithColumnAnnotation =
      fields.flatMap(f => getAnnotationParams[javax.persistence.Column](f).map(ap => (f, ap.toMap)))
    val uniqueColumnConstraints = fieldsWithColumnAnnotation.collect {
      case (field, columnAnnotationParams) if columnAnnotationParams.getOrElse("unique", false) == true =>
        val columnName = getColumnName(field)
        s"ALTER TABLE $qualifiedTableName ADD CONSTRAINT ${fullTableName}_$columnName UNIQUE ($columnName);"
    }
    val idColumnNames = fields.collect {
      case field if isAnnotated[javax.persistence.Id](field) => getColumnName(field)
    }
    val uniqueKeyConstraint = Option(idColumnNames).filter(_.nonEmpty).map(columnNames =>
      s"ALTER TABLE $qualifiedTableName ADD PRIMARY KEY (${columnNames.mkString(", ")});")
    uniqueColumnConstraints ++ uniqueKeyConstraint
  }

  // Note: This may need some work per db type. It seems fine for mysql, postgres and h2.
  private def getSqlTypeName(field: Symbol): String = {
    val column = getAnnotationParams[javax.persistence.Column](field).getOrElse(Seq.empty).toMap
    val columnNullable = column.get("nullable").asInstanceOf[Option[Boolean]]
    val lob = getAnnotationParams[javax.persistence.Lob](field)

    val rawFieldType = field.typeSignature.dealias
    val isOption = isSubtype(rawFieldType, localTypeOf[Option[_]])
    val fieldType = if (isOption) rawFieldType.typeArgs.head else rawFieldType

    val isPrimitive = rawFieldType.typeSymbol.asClass.isPrimitive
    val nullable = columnNullable.getOrElse(!isPrimitive)

    val maybeNotNullSuffix = if (nullable) "" else " NOT NULL"
    val fullSqlType = if (column.contains("columnDefinition")) {
      column("columnDefinition")
    } else if (fieldType =:= typeOf[String] && lob.isDefined) {
      s"TEXT$maybeNotNullSuffix"
    } else {
      val sqlType = fieldType match {
        case t if t =:= definitions.BooleanTpe => "BOOLEAN"
//        case t if t =:= definitions.ByteTpe => "TINYINT UNSIGNED"
//        case t if t =:= definitions.ShortTpe => "SMALLINT"
//        case t if t =:= definitions.CharTpe => "CHAR"
        case t if t =:= definitions.IntTpe => "INT"
        case t if t =:= definitions.LongTpe => "BIGINT"
        case t if t =:= definitions.FloatTpe => "FLOAT"
        case t if t =:= definitions.DoubleTpe => "DOUBLE"
        case t if t =:= localTypeOf[String] => s"VARCHAR(${column.getOrElse("length", DEFAULT_COLUMN_LENGTH)})"
        case t if t =:= localTypeOf[java.sql.Date] => "DATE"
        case t if isSubtype(t, localTypeOf[Iterable[_]]) => "JSON"
        case t if isCaseClass(t) => "JSON"
        case _ => throw new SToysException(s"Unsupported type ${renderAnnotatedType(field.typeSignature)}!")
      }
      s"$sqlType$maybeNotNullSuffix"
    }
    s"${getColumnName(field)} $fullSqlType"
  }
}

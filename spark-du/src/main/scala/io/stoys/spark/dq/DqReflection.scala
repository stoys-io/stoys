package io.stoys.spark.dq

import io.stoys.scala.{Reflection, Strings}
import io.stoys.spark.SToysException

import scala.reflect.runtime.universe._

object DqReflection {
  import Reflection._

  def getDqFields[T <: Product : TypeTag]: Seq[DqField] = {
    getCaseClassFields[T].map(getDqField)
  }

  private def getDqField(field: Symbol): DqField = {
    val dqField = getAnnotationParamsMap[annotation.DqField](field)
    val dqFieldNullable = dqField.get("nullable").asInstanceOf[Option[Boolean]]
    val dqFieldEnumValues = dqField.getOrElse("enumValues", Seq.empty[String]).asInstanceOf[Seq[String]]
    val dqFieldFormat = dqField.get("format").asInstanceOf[Option[String]]
    val dqFieldRegexp = dqField.get("regexp").asInstanceOf[Option[String]]

    val rawFieldType = field.typeSignature.dealias
    val isOption = isSubtype(rawFieldType, localTypeOf[Option[_]])
    val fieldType = if (isOption) rawFieldType.typeArgs.head else rawFieldType

    val isPrimitive = rawFieldType.typeSymbol.asClass.isPrimitive
    val nullable = dqFieldNullable.getOrElse(!isPrimitive)

    val typ = fieldType match {
      case t if t =:= definitions.BooleanTpe => "boolean"
//      case t if t =:= definitions.ByteTpe => "byte"
//      case t if t =:= definitions.ShortTpe => ???
//      case t if t =:= definitions.CharTpe => "char"
      case t if t =:= definitions.IntTpe => "integer"
      case t if t =:= definitions.LongTpe => "long"
      case t if t =:= definitions.FloatTpe => "float"
      case t if t =:= definitions.DoubleTpe => "double"
      case t if t =:= typeOf[String] => "string"
      case t if t =:= typeOf[java.sql.Date] => "date"
      case t if t =:= typeOf[java.sql.Timestamp] => "timestamp"
//      case t if isSubtype(t, typeOf[Map[_, _]]) => "map"
//      case t if isSubtype(t, typeOf[Iterable[_]]) => "array"
      case _ => throw new SToysException(s"Unsupported type ${renderAnnotatedType(field.typeSignature)}!")
    }

    DqField(getColumnName(field), typ, nullable, dqFieldEnumValues, dqFieldFormat, dqFieldRegexp)
  }

  private def getColumnName(symbol: Symbol): String = {
    Strings.toSnakeCase(nameOf(symbol))
  }
}

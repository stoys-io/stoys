package io.stoys.spark.dq

import io.stoys.scala.{Reflection, Strings}
import io.stoys.spark.SToysException

import scala.reflect.runtime.universe._

object DqReflection {
  def getDqFields[T <: Product: TypeTag]: Seq[DqField] = {
    Reflection.getCaseClassFields[T].map(getDqField)
  }

  def getDqField(field: Symbol): DqField = {
    val explicitParams = Reflection.getAnnotationParams[annotation.DqField](field).getOrElse(Seq.empty).toMap
    val nullable = explicitParams.get("nullable").asInstanceOf[Option[Boolean]]
    val enumValues = explicitParams.getOrElse("enumValues", Seq.empty[String]).asInstanceOf[Seq[String]]
    val format = explicitParams.get("format").asInstanceOf[Option[String]]
    val regexp = explicitParams.get("regexp").asInstanceOf[Option[String]]

    val rawFieldType = field.typeSignature.dealias
    val isOption = rawFieldType <:< typeOf[Option[_]]
    val fieldType = if (isOption) rawFieldType.typeArgs.head else rawFieldType
    val isPrimitive = fieldType.typeSymbol.asClass.isPrimitive

    val typ = fieldType match {
      case t if t =:= typeOf[Boolean] => "boolean"
//      case t if t =:= typeOf[Byte] => "byte"
//      case t if t =:= typeOf[Short] => ???
//      case t if t =:= typeOf[Char] => "char"
      case t if t =:= typeOf[Int] => "integer"
      case t if t =:= typeOf[Long] => "long"
      case t if t =:= typeOf[Float] => "float"
      case t if t =:= typeOf[Double] => "double"
      case t if t =:= typeOf[String] => "string"
      case t if t =:= typeOf[java.sql.Date] => "date"
      case t if t =:= typeOf[java.sql.Timestamp] => "timestamp"
//      case t if t <:< typeOf[Map[_, _]] => "map"
//      case t if t <:< typeOf[Iterable[_]] => "array"
      case _ => throw new SToysException(s"Unsupported type ${Reflection.renderAnnotatedSymbol(field)}!")
    }

    DqField(getColumnName(field), typ, nullable.getOrElse(!isPrimitive || isOption), enumValues, format, regexp)
  }

  private def getColumnName(symbol: Symbol): String = {
    Strings.toSnakeCase(Reflection.termNameOf(symbol))
  }
}

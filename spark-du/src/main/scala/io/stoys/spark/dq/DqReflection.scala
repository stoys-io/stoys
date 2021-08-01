package io.stoys.spark.dq

import io.stoys.scala.Reflection
import io.stoys.spark.SToysException
import org.apache.spark.sql.catalyst.ScalaReflection

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

object DqReflection {
  def getDqFields[T <: Product : TypeTag]: Seq[DqField] = {
    getDqFields[T]()
  }

  def getDqFields[T <: Product : TypeTag](ignoreUnsupportedTypes: Boolean = false): Seq[DqField] = {
    Reflection.getCaseClassFields[T].flatMap(f => getDqField(f, ignoreUnsupportedTypes))
  }

  private def getDqField(field: Symbol, ignoreUnsupportedTypes: Boolean): Option[DqField] = {
    val dqField = Reflection.getAnnotationParamsMap[annotation.DqField](field)
    val ignore = dqField.getOrElse("ignore", false).asInstanceOf[Boolean]
    val nullable = dqField.getOrElse("nullable", true).asInstanceOf[Boolean]
    val enumValues = dqField.getOrElse("enumValues", Seq.empty[String]).asInstanceOf[Seq[String]]
    val format = dqField.get("format").asInstanceOf[Option[String]]
    val regexp = dqField.get("regexp").asInstanceOf[Option[String]]

    if (ignore) {
      None
    } else {
      Try(ScalaReflection.schemaFor(field.typeSignature)) match {
        case Success(schema) =>
          Some(DqField(getFieldName(field), schema.dataType.json, nullable, enumValues, format, regexp))
        case Failure(e) if !ignoreUnsupportedTypes =>
          throw new SToysException(s"Unsupported type ${Reflection.renderAnnotatedType(field.typeSignature)}!", e)
        case Failure(_) => None
      }
    }
  }

  private def getFieldName(field: Symbol): String = {
    Reflection.nameOf(field)
  }
}

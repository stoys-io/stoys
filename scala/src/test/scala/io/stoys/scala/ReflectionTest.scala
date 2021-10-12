package io.stoys.scala

import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe._

class ReflectionTest extends AnyFunSuite {
  import Reflection._
  import ReflectionTest._

  private val record = Record("foo", 42, NestedRecord(42.0))
  private val recordsFields = typeTag[Record].tpe.typeSymbol.asClass.primaryConstructor.asMethod.paramLists.flatten

  test("localTypeOf") {
    assert(localTypeOf[Record].typeSymbol.name.toString === "Record")
    assert(localTypeOf[AnnotatedRecordAlias] === localTypeOf[Record])
  }

  test("typeSymbolOf") {
    assert(typeSymbolOf[Record] === symbolOf[Record])
    assert(typeSymbolOf(typeOf[Record]) === symbolOf[Record])
    assert(typeSymbolOf[AnnotatedRecordAlias] === typeSymbolOf[Record])
  }

  test("nameOf") {
    assert(nameOf(symbolOf[Record]) === "Record")
    assert(nameOf(symbolOf[AnnotatedRecordAlias]) === "AnnotatedRecordAlias")

    assert(recordsFields.map(nameOf) === Seq("s", "i", "nested"))
  }

  test("typeNameOf") {
    assert(typeNameOf[Record] === "Record")
    assert(typeNameOf(typeOf[Record]) === "Record")
    assert(typeNameOf(symbolOf[Record]) === "Record")
    assert(typeNameOf[AnnotatedRecordAlias] === typeNameOf[Record])

    assert(recordsFields.map(typeNameOf) === Seq("String", "Int", "NestedRecord"))
  }

  test("fullTypeNameOf") {
    assert(fullTypeNameOf[Record] === classOf[Record].getCanonicalName)
    assert(fullTypeNameOf(typeOf[Record]) === classOf[Record].getCanonicalName)
    assert(fullTypeNameOf(symbolOf[Record]) === classOf[Record].getCanonicalName)
    assert(fullTypeNameOf[AnnotatedRecordAlias] === fullTypeNameOf[Record])

    assert(recordsFields.map(fullTypeNameOf)
        === Seq("java.lang.String", "scala.Int", classOf[NestedRecord].getCanonicalName))
  }

  test("isCaseClass") {
    assert(isCaseClass[Record] === true)
    assert(isCaseClass(typeOf[Record]) === true)
    assert(isCaseClass(symbolOf[Record]) === true)
    assert(isCaseClass[RegularClass] === false)
    assert(isCaseClass[AnnotatedRecordAlias] === isCaseClass[Record])
  }

  test("isAnnotated") {
    assert(isAnnotated[Record, TestAnnotation] === true)
    assert(isAnnotated[TestAnnotation](symbolOf[Record]) === true)
    assert(isAnnotated(symbolOf[Record], typeOf[TestAnnotation]) === true)
    assert(isAnnotated[NestedRecord, TestAnnotation] === false)
    assert(isAnnotated[RegularClass, TestAnnotation] === true)
    assert(isAnnotated[AnnotatedRecordAlias, TestAnnotation] === isAnnotated[Record, TestAnnotation])

    assert(recordsFields.filter(f => isAnnotated[TestAnnotation](f)).map(_.name.toString) === Seq("s", "i"))
  }

  test("assertAnnotatedCaseClass") {
    assertCaseClass[Record]()
    assertCaseClass(symbolOf[Record])
    assertCaseClass(typeOf[Record])
    assertAnnotated[Record, TestAnnotation]()
    assertAnnotatedCaseClass[Record, TestAnnotation]()
    assertAnnotatedCaseClass[TestAnnotation](symbolOf[Record])
    assertAnnotatedCaseClass[TestAnnotation](typeOf[Record])

    assertCaseClass[NestedRecord]()
    assertThrows[IllegalArgumentException](assertAnnotated[NestedRecord, TestAnnotation]())
    assertThrows[IllegalArgumentException](assertAnnotatedCaseClass[NestedRecord, TestAnnotation]())

    assertThrows[IllegalArgumentException](assertCaseClass[RegularClass]())
    assertAnnotated[RegularClass, TestAnnotation]()
    assertThrows[IllegalArgumentException](assertAnnotatedCaseClass[NestedRecord, TestAnnotation]())

    assertCaseClass[AnnotatedRecordAlias]()
    assertAnnotated[AnnotatedRecordAlias, TestAnnotation]()
    assertAnnotatedCaseClass[AnnotatedRecordAlias, TestAnnotation]()
  }

  test("getCaseClassFields") {
    assert(getCaseClassFields[Record] === recordsFields)
    assert(getCaseClassFields[Record].map(s => s.name.toString -> s.info.toString)
        === Seq("s" -> "String", "i" -> "Int", "nested" -> classOf[NestedRecord].getCanonicalName))
    assert(getCaseClassFields[AnnotatedRecordAlias] === getCaseClassFields[Record])
  }

  test("getCaseClassFieldNames") {
    assert(getCaseClassFieldNames[Record] === Seq("s", "i", "nested"))
    assert(getCaseClassFieldNames[AnnotatedRecordAlias] === getCaseClassFieldNames[Record])
  }

  test("createCaseClassInstance") {
    assert(createCaseClassInstance[Record](Seq("foo", 42, NestedRecord(42.0))) === record)
    assertThrows[RuntimeException](createCaseClassInstance[Record](Seq("missingArgs")))
    assertThrows[RuntimeException](createCaseClassInstance[Record](Seq(42, "wrongOrderArgs", NestedRecord(42.0))))
    assert(createCaseClassInstance[AnnotatedRecordAlias](Seq("foo", 42, NestedRecord(42.0))) === record)
  }

  test("enumerationValuesOf") {
    val enumerationValues = enumerationValuesOf[FooBarBaz.FooBarBaz]
    assert(enumerationValues.map(_.toString) === Seq("FOO", "BAR", "BAZ"))
    assert(enumerationValues === enumerationValuesOf(typeOf[FooBarBaz.FooBarBaz]))
    assertThrows[ScalaReflectionException](enumerationValuesOf(typeOf[Record]))
  }

  test("getFieldValue") {
    assert(getFieldValue(record, "s") === "foo")
    assert(getFieldValue(record, "i") === 42)
    assert(getFieldValue(record, "nested") === NestedRecord(42.0))
    assertThrows[NoSuchFieldException](getFieldValue(record, "nonExistingField"))
    // TODO: Should we add support for recursive field extraction?
    assertThrows[NoSuchFieldException](getFieldValue(record, "nested.d"))
  }

  test("getAnnotationParams") {
    assert(getAnnotationParams[Record, TestAnnotationValue] === None)
    assert(getAnnotationParams[Record, TestAnnotation] === Some(Seq.empty))
    assert(getAnnotationParams[TestAnnotation](symbolOf[Record]) === Some(Seq.empty))
    // We cannot dealias getAnnotationParams as we would break it for fields.
    assert(getAnnotationParams[TestAnnotation](symbolOf[AnnotatedRecordAlias]) === None)

    assert(getAnnotationParams[NotAnnotated, TestAnnotation] === None)
    assert(getAnnotationParams[AnnotatedNoParams, TestAnnotation] === Some(Seq.empty))
    assert(getAnnotationParams[AnnotatedExplicitParams, TestAnnotation] === Some(Seq(
      "stringValue" -> "overridden",
      "arrayValue" -> Seq(4, 2),
      "enumValue" -> TestEnum.BAR,
      "classValue" -> classOf[Record],
      "annotationValue" -> Seq("value" -> "foo"))))
  }

  test("getAnnotationParamsMap") {
    assert(getAnnotationParamsMap[Record, TestAnnotationValue] === Map.empty)
    assert(getAnnotationParamsMap[Record, TestAnnotation] === Map.empty)
  }

  test("getAllAnnotationsParamsMap") {
    assert(recordsFields.map(getAllAnnotationsParamsMap) === Seq(
      Map("io.stoys.scala.TestAnnotation" -> Map.empty, "io.stoys.scala.TestAnnotationValue" -> Map("value" -> "foo")),
      Map("io.stoys.scala.TestAnnotation" -> Map.empty),
      Map.empty))
  }

  test("renderAnnotatedType") {
    assert(renderAnnotatedType[NotAnnotated] === "NotAnnotated")
    assert(renderAnnotatedType[AnnotatedNoParams] === "@TestAnnotation() AnnotatedNoParams")
    assert(renderAnnotatedType[AnnotatedExplicitParams] === "@TestAnnotation(" +
        "stringValue = \"overridden\"," +
        " arrayValue = List(4, 2)," +
        " enumValue = BAR," +
        " classValue = class io.stoys.scala.ReflectionTest$Record," +
        " annotationValue = List((value,foo))" +
        ") AnnotatedExplicitParams")
    assert(renderAnnotatedType(typeOf[AnnotatedRecordAlias]) === renderAnnotatedType[Record])
  }

  test("classNameToTypeTag") {
    assert(classNameToTypeTag(classOf[Record].getName).tpe =:= typeTag[Record].tpe)
    assert(getCaseClassFields(classNameToTypeTag(classOf[Record].getName)) === getCaseClassFields[Record])
  }

  test("copyCaseClass") {
    assert(copyCaseClass(NestedRecord(42.0), Map("d" -> Double.box(1234.0))) === NestedRecord(1234.0))
    assertThrows[Exception](copyCaseClass(NestedRecord(42.0), Map("d" -> "1234")))

    val record = Record("foo", 42, NestedRecord(42.0))
    val overrides = Map("s" -> "bar", "nested" -> NestedRecord(1234.0))
    assert(copyCaseClass(record, overrides) === Record("bar", 42, NestedRecord(1234.0)))
  }
}

object ReflectionTest {
  @TestAnnotation
  case class Record(
      @TestAnnotation @TestAnnotationValue("foo")
      s: String,
      @TestAnnotation
      i: Int,
      nested: NestedRecord,
  )

  case class NestedRecord(d: Double)

  type AnnotatedRecordAlias = Record@TestAnnotation

  @TestAnnotation
  class RegularClass

  object FooBarBaz extends Enumeration {
    type FooBarBaz = Value
    val FOO, BAR, BAZ = Value
  }

  case class NotAnnotated(s: String)

  @TestAnnotation
  case class AnnotatedNoParams(s: String)

  @TestAnnotation(
    stringValue = "overridden",
    arrayValue = Array(4, 2),
    enumValue = TestEnum.BAR,
    classValue = classOf[Record],
    annotationValue = new TestAnnotationValue(value = "foo"))
  case class AnnotatedExplicitParams(s: String)
}

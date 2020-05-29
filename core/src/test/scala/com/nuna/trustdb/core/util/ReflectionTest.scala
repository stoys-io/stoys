package com.nuna.trustdb.core.util

import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe._

class ReflectionTest extends AnyFunSuite {
  import Reflection._
  import ReflectionTest._

  test("dealiasedTypeOf") {
    assert(dealiasedTypeOf[Record].typeSymbol.name.toString === "Record")
    assert(dealiasedTypeOf[Record] === typeOf[Record])
    assert(dealiasedTypeOf[RecordAlias] === dealiasedTypeOf[Record])
  }

  test("dealiasedTypeSymbolOf") {
    assert(dealiasedTypeSymbolOf[Record].name.toString === "Record")
    assert(dealiasedTypeSymbolOf[Record] === symbolOf[Record])
    assert(dealiasedTypeSymbolOf(symbolOf[Record]) === symbolOf[Record])
    assert(dealiasedTypeSymbolOf[RecordAlias] === dealiasedTypeSymbolOf[Record])
    assert(dealiasedTypeSymbolOf(symbolOf[RecordAlias]) === dealiasedTypeSymbolOf(symbolOf[Record]))
  }

  test("typeNameOf") {
    assert(typeNameOf[Record] === "Record")
    assert(typeNameOf(typeOf[Record]) === "Record")
    assert(typeNameOf(symbolOf[Record]) === "Record")

    assert(typeNameOf[NestedRecord] === "NestedRecord")
    assert(typeNameOf[RegularClass] === "RegularClass")

    assert(typeNameOf[RecordAlias] === typeNameOf[Record])
    assert(typeNameOf(typeOf[RecordAlias]) === typeNameOf(typeOf[Record]))
    assert(typeNameOf(symbolOf[RecordAlias]) === typeNameOf(symbolOf[Record]))

    assert(getCaseClassFields[Record].map(typeNameOf) === Seq("String", "Int", "NestedRecord"))
    assert(getCaseClassFields[NestedRecord].map(typeNameOf) === Seq("Double"))
    assert(getCaseClassFields[RecordAlias].map(typeNameOf) === Seq("String", "Int", "NestedRecord"))
  }

  test("termNameOf") {
    assert(termNameOf(symbolOf[Record]) === "Record")

    assert(getCaseClassFields[Record].map(termNameOf) === Seq("s", "i", "nested"))
    assert(getCaseClassFields[NestedRecord].map(termNameOf) === Seq("d"))
    assert(getCaseClassFields[RecordAlias].map(termNameOf) === Seq("s", "i", "nested"))
  }

  test("isCaseClass") {
    assert(isCaseClass[Record] === true)
    assert(isCaseClass(typeOf[Record]) === true)
    assert(isCaseClass(symbolOf[Record]) === true)

    assert(isCaseClass[NestedRecord] === true)
    assert(isCaseClass[RegularClass] === false)

    assert(isCaseClass[RecordAlias] === isCaseClass[Record])
    assert(isCaseClass(typeOf[RecordAlias]) === isCaseClass(typeOf[Record]))
    assert(isCaseClass(symbolOf[RecordAlias]) === isCaseClass(symbolOf[Record]))
  }

  test("isAnnotated") {
    assert(isAnnotated[Record, TestAnnotation] === true)
    assert(isAnnotated[NestedRecord, TestAnnotation] === false)
    assert(isAnnotated[RegularClass, TestAnnotation] === true)
    assert(isAnnotated[RecordAlias, TestAnnotation] === isAnnotated[Record, TestAnnotation])
  }

  test("assertAnnotatedCaseClass") {
    assertCaseClass[Record]
    assertAnnotated[Record, TestAnnotation]
    assertAnnotatedCaseClass[Record, TestAnnotation]

    assertCaseClass[NestedRecord]
    assertThrows[IllegalArgumentException](assertAnnotated[NestedRecord, TestAnnotation])
    assertThrows[IllegalArgumentException](assertAnnotatedCaseClass[NestedRecord, TestAnnotation])

    assertThrows[IllegalArgumentException](assertCaseClass[RegularClass])
    assertAnnotated[RegularClass, TestAnnotation]
    assertThrows[IllegalArgumentException](assertAnnotatedCaseClass[NestedRecord, TestAnnotation])

    assertCaseClass[RecordAlias]
    assertAnnotated[RecordAlias, TestAnnotation]
    assertAnnotatedCaseClass[RecordAlias, TestAnnotation]
  }

  test("getCaseClassFields") {
    val recordFields = getCaseClassFields[Record]
    val recordFieldSimplified = recordFields.map(s => s.name.toString -> s.info.toString)
    assert(recordFieldSimplified === Seq(
      "s" -> "String", "i" -> "Int", "nested" -> classOf[NestedRecord].getName.replace('$', '.')))
    assert(getCaseClassFields[RecordAlias] === getCaseClassFields[Record])
  }

  test("getCaseClassFieldNames") {
    assert(getCaseClassFieldNames[Record] === Seq("s", "i", "nested"))
    assert(getCaseClassFieldNames[RecordAlias] === getCaseClassFieldNames[Record])
  }

  test("createCaseClassInstance") {
    assert(createCaseClassInstance[Record](Seq("foo", 1, NestedRecord(0.0))) === Record("foo", 1, NestedRecord(0.0)))
    assertThrows[RuntimeException](createCaseClassInstance[Record](Seq("missingArgs")))
    assertThrows[RuntimeException](createCaseClassInstance[Record](Seq(1, "wrongOrderArgs", NestedRecord(0.0))))
  }

  test("getEnumerationValues") {
    val enumerationValues = getEnumerationValues(typeToTypeTag(typeOf[FooBarBaz.FooBarBaz]))
    assert(enumerationValues.map(_.toString) === Seq("FOO", "BAR", "BAZ"))
    assertThrows[ScalaReflectionException](getEnumerationValues(typeToTypeTag(typeOf[Record])))
  }

  test("getFieldValue") {
    val record = Record("foo", 42, NestedRecord(42.0))
    assert(getFieldValue(record, "s") === "foo")
    assert(getFieldValue(record, "i") === 42)
    assert(getFieldValue(record, "nested") === NestedRecord(42.0))
    assertThrows[NoSuchFieldException](getFieldValue(record, "nonExistingField"))
    // TODO: Should we add support for recursive field extraction?
    assertThrows[NoSuchFieldException](getFieldValue(record, "nested.d"))
  }

  test("getAnnotationParams") {
    assert(getAnnotationParams[TestAnnotation](symbolOf[Record]) === Some(Seq.empty))
    // We cannot dealias getAnnotationParams as we would break it for fields.
    assert(getAnnotationParams[TestAnnotation](symbolOf[RecordAlias]) === None)

    assert(getAnnotationParams[TestAnnotation](symbolOf[NotAnnotated]) === None)
    assert(getAnnotationParams[TestAnnotation](symbolOf[AnnotatedNoParams]) === Some(Seq.empty))
    assert(getAnnotationParams[TestAnnotation](symbolOf[AnnotatedExplicitParams])
        === Some(Seq("stringValue" -> "overridden", "arrayValue" -> Seq(4, 2), "enumValue" -> TestEnum.BAR,
      "classValue" -> classOf[Record])))
  }

  test("renderAnnotatedSymbol") {
    assert(renderAnnotatedSymbol(symbolOf[NotAnnotated]) === "NotAnnotated")
    assert(renderAnnotatedSymbol(symbolOf[AnnotatedNoParams]) === "@TestAnnotation() AnnotatedNoParams")
    assert(renderAnnotatedSymbol(symbolOf[AnnotatedExplicitParams])
        === "@TestAnnotation(stringValue = \"overridden\", arrayValue = List(4, 2), enumValue = BAR,"
        + " classValue = class com.nuna.trustdb.core.util.ReflectionTest$Record) AnnotatedExplicitParams")
  }

  test("classNameToTypeTag") {
    assert(classNameToTypeTag(classOf[Record].getName).tpe =:= typeTag[Record].tpe)
    assert(getCaseClassFields(classNameToTypeTag(classOf[Record].getName)) === getCaseClassFields(typeTag[Record]))
  }
}

object ReflectionTest {
  @TestAnnotation
  case class Record(s: String, i: Int, nested: NestedRecord)

  case class NestedRecord(d: Double)

  type RecordAlias = Record

  @TestAnnotation
  class RegularClass

  object FooBarBaz extends Enumeration {
    type FooBarBaz = Value
    val FOO, BAR, BAZ = Value
  }

  case class NotAnnotated(value: String)

  @TestAnnotation
  case class AnnotatedNoParams(value: String)

  // TODO: What si correct scala syntax for: annotationValue = @TestAnnotationValue(value = "foo")
  @TestAnnotation(stringValue = "overridden", arrayValue = Array(4, 2), enumValue = TestEnum.BAR,
    classValue = classOf[Record])
  case class AnnotatedExplicitParams(value: String)
}

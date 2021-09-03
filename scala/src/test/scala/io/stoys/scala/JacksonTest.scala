package io.stoys.scala

import org.scalatest.funsuite.AnyFunSuite

class JacksonTest extends AnyFunSuite {
  import Jackson._
  import JacksonTest._

  test("deepCopy") {
    val originalRecord = Record("foo", Seq("foo"), Map("foo" -> "foo"), NestedRecord(Some("nested")))
    val clonedRecord = deepCopy(originalRecord)
    assert(clonedRecord === originalRecord)
    assert(json.updateValue(clonedRecord, "{\"str\": \"bar\"}") === originalRecord.copy(str = "bar"))
    assert(clonedRecord === originalRecord.copy(str = "bar"))
    assert(originalRecord === originalRecord)
  }

//  test("empty") {
//    assert(empty[Record] === Record(null, Seq.empty, Map.empty, NestedRecord(None)))
//  }
}

object JacksonTest {
  case class NestedRecord(option: Option[String])
  case class Record(str: String, seq: Seq[String], map: Map[String, String], nested: NestedRecord)
}

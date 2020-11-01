package io.stoys.spark

import org.scalatest.funsuite.AnyFunSuite

class TableNameLookupTest extends AnyFunSuite {
  import TableNameLookupTest._

  test("fullTableName") {
    assert(TableName[NyanCat].fullTableName() === "nyan_cat")
    assert(TableName[NyanCat]("bff").fullTableName() === "nyan_cat__bff")
    assert(TableName[NyanCat]("=^_^= meows").fullTableName() === "nyan_cat___meows")
    assert(TableName[NyanCat]("likes.no.dots").fullTableName() === "nyan_cat__likes_no_dots")
  }

  test("parse") {
    val tableNameLookup = new TableNameLookup(Seq(classOf[NyanCat]))
    val tableName = TableName[NyanCat]("nyan_cat", None)
    assert(tableNameLookup.parse[NyanCat]("nyan_cat") === Some(tableName.copy(logicalName = None)))
    assert(tableNameLookup.parse[NyanCat]("nyan_cat__meows") === Some(tableName.copy(logicalName = Some("meows"))))
    assert(tableNameLookup.parse[NyanCat]("nyan_cat___meows") === Some(tableName.copy(logicalName = Some("_meows"))))
    assert(tableNameLookup.parse[NyanCat]("nyan_cat____meows") === None)
    assert(tableNameLookup.parse[NyanCat]("nyan_cat__meows__forever") === None)
    assert(tableNameLookup.parse[NyanCat]("nyan_dog") === None)
  }
}

object TableNameLookupTest {
  case class NyanCat(value: String)
}

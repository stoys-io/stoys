package io.stoys.spark

import org.scalatest.funsuite.AnyFunSuite

class TableNameLookupTest extends AnyFunSuite {
  import TableNameLookupTest._

  test("lookupEntityTableName") {
    val tableNameLookup = new TableNameLookup(Seq(classOf[NyanCat]))
    val tableName = TableName[NyanCat](Some("nyan_cat"), None)
    assert(tableNameLookup.lookupEntityTableName[NyanCat]("nyan_cat")
        === Some(tableName.copy(logicalName = None)))
    assert(tableNameLookup.lookupEntityTableName[NyanCat]("nyan_cat__meows")
        === Some(tableName.copy(logicalName = Some("meows"))))
    assert(tableNameLookup.lookupEntityTableName[NyanCat]("nyan_cat___meows")
        === Some(tableName.copy(logicalName = Some("_meows"))))
    assert(tableNameLookup.lookupEntityTableName[NyanCat]("nyan_cat____meows")
        === Some(tableName.copy(logicalName = Some("__meows"))))
    assert(tableNameLookup.lookupEntityTableName[NyanCat]("nyan_cat__meows__forever")
        === Some(tableName.copy(logicalName = Some("meows__forever"))))
    assert(tableNameLookup.lookupEntityTableName[NyanCat]("nyan_dog") === None)
    assert(tableNameLookup.lookupEntityTableName[NyanCat]("__logical_name_only") === None)
  }

  test("lookupTableName") {
    val tableNameLookup = new TableNameLookup(Seq(classOf[NyanCat]))
    assert(tableNameLookup.lookupTableName("__logical_name_only")
        === Some(TableName(None, Some("logical_name_only"))))
    assert(tableNameLookup.lookupEntityTableName[NyanCat]("__logical_name_only") === None)
  }
}

object TableNameLookupTest {
  case class NyanCat(name: String)
}

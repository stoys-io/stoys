package io.stoys.spark

import io.stoys.spark.test.SparkTestBase

class TableNameTest extends SparkTestBase {
  import TableNameTest._

  test("fullTableName") {
    assert(TableName[NyanCat].fullTableName() === "nyan_cat")
    assert(TableName[NyanCat]("bff").fullTableName() === "nyan_cat__bff")
    assert(TableName[NyanCat]("=^_^= meows").fullTableName() === "nyan_cat___meows")
    assert(TableName[NyanCat]("likes.no.dots").fullTableName() === "nyan_cat__likes_no_dots")
  }

  test("of") {
    import sparkSession.implicits._
    val nyanCat = Seq(NyanCat("meow")).toDS()
    assert(TableName.of(nyanCat).fullTableName() === "nyan_cat")
    assert(TableName.of(nyanCat.as("nyan_cat__alias")).fullTableName() === "nyan_cat__alias")
    val tableNameOfNyanCatDf = TableName.of(nyanCat.toDF())
    assert(tableNameOfNyanCatDf.entityName === None)
    assert(tableNameOfNyanCatDf.logicalName.filterNot(_.matches("^[0-9a-f]{7}__\\d+$")) === None)
  }
}

object TableNameTest {
  case class NyanCat(name: String)
}

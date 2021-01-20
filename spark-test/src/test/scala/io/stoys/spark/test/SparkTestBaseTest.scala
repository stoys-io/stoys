package io.stoys.spark.test

class SparkTestBaseTest extends SparkTestBase {
  import SparkTestBaseTest._
  import sparkSession.implicits._

  test("file utilities") {
    assert(tmpDir.toString.contains(this.getClass.getSimpleName))
    val records = Seq(Record("foo", 42))
    val genericRecords = records.flatMap(Record.unapply)
    assert(records.size === genericRecords.size)
    writeData(s"$tmpDir/writeData", records)
    writeDataFrame(s"$tmpDir/writeDataFrame", genericRecords.toDF())
    writeDataset(s"$tmpDir/writeDataset", records.toDS())
    assert(readData[Record](s"$tmpDir/writeData") === records)
    assert(readDataFrame(s"$tmpDir/writeData").collect().map(_.toSeq) === genericRecords.map(_.productIterator.toSeq))
    assert(readDataset[Record](s"$tmpDir/writeData").collect() === records)
    val statuses = walkDfsFileStatusesByRelativePath(tmpDir.toString)
    assert(statuses.size === 3)
    assert(statuses.keySet.filterNot(_.matches("writeData.*parquet")) === Set.empty)
  }
}

object SparkTestBaseTest {
  case class Record(s: String, i: Int)
}

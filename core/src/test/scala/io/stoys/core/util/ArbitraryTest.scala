package io.stoys.core.util

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import org.scalatest.funsuite.AnyFunSuite

class ArbitraryTest extends AnyFunSuite {
  import Arbitrary._
  import ArbitraryTest._

  test("Primitives") {
    val expectedHashed0 =
      Primitives(true, -43, 18627, 1241660790, 3101505174803136092L, -4.8045547E18f, -3.6953587070444283E18)
    val expectedHashed1 =
      Primitives(false, 56, 12731, 996725495, 8749419802067486576L, 6.1406004E18f, -7.4158343478326579E18)
    val expectedRandom42 =
      Primitives(true, 40, -17589, -1255373459, -6169532649852302182L, -1.78246695E18f, 6.8028440265634191E18)

    assert(constant[Primitives](42) === Primitives(false, 42, 42, 42, 42L, 42.0f, 42.0d))
    assert(default[Primitives] === Primitives(false, 0, 0, 0, 0L, 0.0f, 0.0d))
    assert(empty[Primitives] === Primitives(false, 0, 0, 0, 0L, 0.0f, 0.0d))
    assert(indexed[Primitives](0) === Primitives(false, 1, 2, 3, 4L, 5.0f, 6.0d))
    assert(hashed[Primitives](0) === expectedHashed0)
    assert(hashed[Primitives](1) === expectedHashed1)
    assert(proto[Primitives] === Primitives(false, 0, 0, 0, 0L, 0.0f, 0.0d))
    assert(random[Primitives](42) === expectedRandom42)

    val indexedArbitrary = new Arbitrary(ArbitraryStrategy.INDEXED, seed = 0)
    assert(indexedArbitrary.make[Primitives] === Primitives(false, 1, 2, 3, 4L, 5.0f, 6.0d))
    assert(indexedArbitrary.make[Primitives] === Primitives(false, -23, 1002, 1003, 1004, 1005.0f, 1006.0))

    val indexedConstantArbitrary = new Arbitrary(ArbitraryStrategy.INDEXED_CONSTANT, seed = 0)
    assert(indexedConstantArbitrary.make[Primitives] === Primitives(false, 0, 0, 0, 0L, 0.0f, 0.0d))
    assert(indexedConstantArbitrary.make[Primitives] === Primitives(true, 1, 1, 1, 1L, 1.0f, 1.0d))

    val hashedArbitrary = new Arbitrary(ArbitraryStrategy.HASHED, seed = 0)
    assert(hashedArbitrary.make[Primitives] === expectedHashed0)
    assert(hashedArbitrary.make[Primitives] === expectedHashed1)

    val hashedConstantArbitrary = new Arbitrary(ArbitraryStrategy.HASHED_CONSTANT)
    assert(hashedConstantArbitrary.make[Primitives] === expectedHashed0)
    assert(hashedConstantArbitrary.make[Primitives] === expectedHashed0)

    val randomArbitrary = new Arbitrary(ArbitraryStrategy.RANDOM, seed = 42)
    assert(randomArbitrary.make[Primitives] === expectedRandom42)
    assert(randomArbitrary.make[Primitives] !== expectedRandom42)
  }

  test("Characters") {
    assert(constant[Characters](42) === Characters(42.toChar, "42"))
    assert(default[Characters] === Characters('\u0000', null))
    assert(empty[Characters] === Characters('\u0000', null))
    assert(indexed[Characters](0) === Characters('\u0000', "1"))
    assert(hashed[Characters](0) === Characters('钝', "-4936050758556305047"))
    assert(proto[Characters] === Characters('\u0000', ""))
    assert(random[Characters](42) === Characters('諷', "-5843495416241995736"))
  }

  test("Enumerations") {
    assert(default[Enumerations] === Enumerations(null))
    assert(empty[Enumerations] === Enumerations(FooBarBaz.FOO))
    assert(hashed[Enumerations](0) === Enumerations(FooBarBaz.BAZ))
    assert(proto[Enumerations] === Enumerations(FooBarBaz.FOO))
  }

  test("Collections") {
    val emptyCollections = Collections(Option.empty, Seq.empty, Set.empty, Map.empty)
    assert(constant[Collections](42) === emptyCollections)
    assert(default[Collections] === Collections(null, null, null, null))
    assert(empty[Collections] === emptyCollections)
    assert(indexed[Collections](0) === emptyCollections)
    assert(hashed[Collections](0) === emptyCollections)
    assert(proto[Collections] === emptyCollections)
    assert(random[Collections](42) === emptyCollections)
  }

  test("Nesting") {
    assert(constant[Nesting](42) === Nesting(Characters('*', "42")))
    assert(default[Nesting] === Nesting(Characters('\u0000', null)))
    assert(empty[Nesting] === Nesting(Characters('\u0000', null)))
    assert(indexed[Nesting](0) === Nesting(Characters('\u0001', "2")))
    assert(hashed[Nesting](0) === Nesting(Characters('钝', "-4936050758556305047")))
    assert(proto[Nesting] === Nesting(Characters('\u0000', "")))
    assert(random[Nesting](42) === Nesting(Characters('쀨', "5694868678511409995")))
  }

  test("JavaTime") {
    assert(constant[JavaTime](42)
        === javaTime("1970-02-12", "00:00:42", "1970-02-12T00:00:42", "1970-02-12T00:00:42Z"))
    assert(default[JavaTime] === JavaTime(null, null, null, null))
    assert(empty[JavaTime] === JavaTime(null, null, null, null))
    assert(indexed[JavaTime](0) === javaTime("1970-01-01", "00:00:01", "1970-01-03T00:00:02", "1970-01-04T00:00:03Z"))
    assert(hashed[JavaTime](0) === javaTime("7656-06-29", "03:28:23", "7550-05-19T13:10", "7101-03-03T22:18:05Z"))
    assert(proto[JavaTime] === JavaTime(null, null, null, null))
    assert(random[JavaTime](42) === javaTime("7452-04-03", "19:51:04", "6799-12-03T09:26:35", "1733-06-09T12:19:57Z"))
  }

  test("JavaSql") {
    assert(constant[JavaSql](42) === javaSql("1970-02-12", "1970-02-12 00:00:42.0"))
    assert(default[JavaSql] === JavaSql(null, null))
    assert(empty[JavaSql] === JavaSql(null, null))
    assert(indexed[JavaSql](0) === javaSql("1970-01-01", "1970-01-02 00:00:01.0"))
    assert(hashed[JavaSql](0) === javaSql("9442-02-07", "3632-04-30 04:26:37.0"))
    assert(proto[JavaSql] === JavaSql(null, null))
    assert(random[JavaSql](42) === javaSql("7452-04-03", "8442-06-10 19:51:04.0"))
  }

  test("SchemaEvolution") {
    assert(hashed[SchemaEvolutionV1](0) === SchemaEvolutionV1(-1467009098, 322257997))
    assert(hashed[SchemaEvolutionV2](0) === SchemaEvolutionV2(29333682, -1467009098, 331146028))
    assert(random[SchemaEvolutionV1](42) === SchemaEvolutionV1(234785527, 205897768))
    assert(random[SchemaEvolutionV2](42) === SchemaEvolutionV2(234785527, 205897768, -248792245))
  }

  test("clamp") {
    assert(clamp(-1, -1, 1) === -1)
    assert(clamp(0, -1, 1) === 0)
    assert(clamp(1, -1, 1) === 1)
    assert(clamp(-2, -1, 1) === 1)
    assert(clamp(2, -1, 1) === -1)
    assert(clamp(-5, -1, 1) === 1)
    assert(clamp(-42, -1, 1) === 0)
  }
}

object ArbitraryTest {
  object FooBarBaz extends Enumeration {
    type FooBarBaz = Value
    val FOO, BAR, BAZ = Value
  }

  case class Primitives(boolean: Boolean, byte: Byte, short: Short, int: Int, long: Long, float: Float, double: Double)
  case class Characters(char: Char, string: String)
  case class Enumerations(enumeration: FooBarBaz.Value) // TODO: sealed traits, java Enum
  case class Collections(option: Option[Int], seq: Seq[Int], set: Set[Int], map: Map[Int, Int])
  case class Nesting(characters: Characters)

  case class JavaTime(localDate: LocalDate, localTime: LocalTime, localDateTime: LocalDateTime, instant: Instant)
  case class JavaSql(date: java.sql.Date, timestamp: java.sql.Timestamp)

  case class SchemaEvolutionV1(foo: Int, bar: Int)
  case class SchemaEvolutionV2(inserted: Int, foo: Int, appended: Int)

  def javaTime(localDate: String, localTime: String, localDateTime: String, instant: String): JavaTime = {
    JavaTime(LocalDate.parse(localDate), LocalTime.parse(localTime), LocalDateTime.parse(localDateTime),
      Instant.parse(instant))
  }

  def javaSql(date: String, timestamp: String): JavaSql = {
    JavaSql(java.sql.Date.valueOf(date), java.sql.Timestamp.valueOf(timestamp))
  }
}

package com.nuna.trustdb.core.util

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import com.nuna.trustdb.core.util.Arbitrary.ArbitraryStrategy
import org.scalatest.funsuite.AnyFunSuite

class ArbitraryTest extends AnyFunSuite {
  import ArbitraryTest._

  test("Primitives") {
    val expectedRandom42 = Primitives(true, -9, -17439, 205897768, 1325939940, -2.4879224E8f, 1.190043011E9)
    val expectedHashed0 = Primitives(false, -42, 13843, 896151886, 722125446, -1.11864755E9f, -8.60392747E8)
    val expectedHashed1 = Primitives(true, -66, 28179, -1200363285, 2037133044, 1.42971994E9f, -1.726633485E9)

    assert(Arbitrary.constant[Primitives](42) === Primitives(false, 42, 42, 42, 42L, 42.0f, 42.0d))
    assert(Arbitrary.empty[Primitives] === Primitives(false, 0, 0, 0, 0L, 0.0f, 0.0d))
    assert(Arbitrary.indexed[Primitives](0) === Primitives(false, 1, 2, 3, 4L, 5.0f, 6.0d))
    assert(Arbitrary.random[Primitives](42) === expectedRandom42)
    assert(Arbitrary.hashed[Primitives](0) === expectedHashed0)
    assert(Arbitrary.hashed[Primitives](1) === expectedHashed1)

    val hashedArbitrary = new Arbitrary(ArbitraryStrategy.HASHED, seed = 0)
    assert(hashedArbitrary.make[Primitives] === expectedHashed0)
    assert(hashedArbitrary.make[Primitives] === expectedHashed1)

    val hashedConstantArbitrary = new Arbitrary(ArbitraryStrategy.HASHED_CONSTANT)
    assert(hashedConstantArbitrary.make[Primitives] === expectedHashed0)
    assert(hashedConstantArbitrary.make[Primitives] === expectedHashed0)

    val indexedArbitrary = new Arbitrary(ArbitraryStrategy.INDEXED, seed = 0)
    assert(indexedArbitrary.make[Primitives] === Primitives(false, 1, 2, 3, 4L, 5.0f, 6.0d))
    assert(indexedArbitrary.make[Primitives] === Primitives(false, -23, 1002, 1003, 1004, 1005.0f, 1006.0))

    val indexedConstantArbitrary = new Arbitrary(ArbitraryStrategy.INDEXED_CONSTANT, seed = 0)
    assert(indexedConstantArbitrary.make[Primitives] === Primitives(false, 0, 0, 0, 0L, 0.0f, 0.0d))
    assert(indexedConstantArbitrary.make[Primitives] === Primitives(true, 1, 1, 1, 1L, 1.0f, 1.0d))

    val randomArbitrary = new Arbitrary(ArbitraryStrategy.RANDOM, seed = 42)
    assert(randomArbitrary.make[Primitives] === expectedRandom42)
    assert(randomArbitrary.make[Primitives] !== expectedRandom42)
  }

  test("Characters") {
    assert(Arbitrary.constant[Characters](42) === Characters(42.toChar, "42"))
    assert(Arbitrary.empty[Characters] === Characters('\u0000', null))
    assert(Arbitrary.indexed[Characters](0) === Characters('\u0000', "1"))
    assert(Arbitrary.random[Characters](42) === Characters('鴵', "234785527"))
    assert(Arbitrary.hashed[Characters](0) === Characters('ᮇ', "-1149263876"))
  }

  test("Enumerations") {
    assert(Arbitrary.empty[Enumerations] === Enumerations(ATEnumeration.FOO))
    assert(Arbitrary.hashed[Enumerations](0) === Enumerations(ATEnumeration.BAR))
  }

  test("Collections") {
    val empty = Collections(Option.empty, Seq.empty, Set.empty, Map.empty)
    assert(Arbitrary.constant[Collections](42) === empty)
    assert(Arbitrary.empty[Collections] === empty)
    assert(Arbitrary.indexed[Collections](0) === empty)
    assert(Arbitrary.random[Collections](42) === empty)
    assert(Arbitrary.hashed[Collections](0) === empty)
  }

  test("Nesting") {
    assert(Arbitrary.constant[Nesting](42) === Nesting(Characters('*', "42")))
    assert(Arbitrary.empty[Nesting] === Nesting(Characters('\u0000', null)))
    assert(Arbitrary.indexed[Nesting](0) === Nesting(Characters('\u0001', "2")))
    assert(Arbitrary.random[Nesting](42) === Nesting(Characters('諷', "-1360544799")))
    assert(Arbitrary.hashed[Nesting](0) === Nesting(Characters('ᮇ', "-1149263876")))
  }

  test("JavaTime") {
    // TODO: JavaTime
  }

  test("JavaSql") {
    def date(s: String): java.sql.Date = java.sql.Date.valueOf(s)

    def timestamp(s: String): java.sql.Timestamp = java.sql.Timestamp.valueOf(s)

    assert(Arbitrary.constant[JavaSql](42) === JavaSql(date("1970-02-12"), timestamp("1970-02-12 00:00:42.0")))
//    assert(Arbitrary.empty[JavaSql] === JavaSql(date("1970-01-01"), timestamp("1970-01-01 00:00:00.0")))
    assert(Arbitrary.empty[JavaSql] === JavaSql(null, null))
    assert(Arbitrary.indexed[JavaSql](0) === JavaSql(date("1970-01-01"), timestamp("1970-01-02 00:00:01.0")))
    assert(Arbitrary.random[JavaSql](42) === JavaSql(date("5730-03-01"), timestamp("2391-03-22 10:57:24.0")))
    assert(Arbitrary.hashed[JavaSql](0) === JavaSql(date("4964-10-05"), timestamp("5241-09-26 01:19:43.0")))
  }

  test("Evolution") {
    assert(Arbitrary.random[EvolutionV1](42) === EvolutionV1(-1170105035, 234785527))
    assert(Arbitrary.random[EvolutionV2](42) === EvolutionV2(-1170105035, 234785527, -1360544799))
    assert(Arbitrary.hashed[EvolutionV1](0) === EvolutionV1(-2093851344, -1424472218))
    assert(Arbitrary.hashed[EvolutionV2](0) === EvolutionV2(1568424554, -1424472218, -625191634))
  }
}

object ArbitraryTest {
  object ATEnumeration extends Enumeration {
    val FOO, BAR, BAZ = Value
  }

  case class Primitives(boolean: Boolean, byte: Byte, short: Short, int: Int, long: Long, float: Float, double: Double)
  case class Characters(char: Char, string: String)
  case class Enumerations(enumeration: ATEnumeration.Value) // TODO: sealed traits, java Enum
  case class Collections(option: Option[Int], seq: Seq[Int], set: Set[Int], map: Map[Int, Int])
  case class Nesting(characters: Characters)
  case class JavaTime(localDate: LocalDate, localTime: LocalTime, localDateTime: LocalDateTime, instant: Instant)
  case class JavaSql(date: java.sql.Date, timestamp: java.sql.Timestamp)

  case class EvolutionV1(foo: Int, bar: Int)
  case class EvolutionV2(renamed: Int, bar: Int, appended: Int)
}

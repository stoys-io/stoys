package com.nuna.trustdb.core.util

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time._
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.runtime.universe._
import scala.util.Random

// Arbitrary.
//
// Reflection based utility to create instances of case classes.
// Use it as an object to create one instance. Use the class when you want to create collection
// (some strategies increment internal counter between instances).
//
// Most useful are:
//   - Arbitrary.empty[T] - create instance with zeros for primitives, empty for collections and null otherwise.
//   - Arbitrary.hashed[T] - (salted) hash of field name (better than random ;-)
//
// Note: It supports only some basic types - the kind of stuff all serialization frameworks do support.
class Arbitrary(strategy: Arbitrary.ArbitraryStrategy.Value, seed: Int = 0) {
  import Arbitrary._

  private val counter = new AtomicInteger(seed)

  def make[T <: Product : TypeTag]: T = {
    strategy match {
      case ArbitraryStrategy.CONSTANT => Arbitrary.constant[T](seed)
      case ArbitraryStrategy.EMPTY => Arbitrary.empty[T]
      case ArbitraryStrategy.HASHED => Arbitrary.hashed[T](counter.getAndIncrement())
      case ArbitraryStrategy.HASHED_CONSTANT => Arbitrary.hashed[T](seed)
      case ArbitraryStrategy.INDEXED => Arbitrary.indexed[T](counter.getAndAdd(1000))
      case ArbitraryStrategy.INDEXED_CONSTANT => Arbitrary.constant[T](counter.getAndIncrement())
      case ArbitraryStrategy.RANDOM => Arbitrary.random[T](counter.getAndIncrement())
    }
  }
}

object Arbitrary {
  object ArbitraryStrategy extends Enumeration {
    val CONSTANT, EMPTY, HASHED, HASHED_CONSTANT, INDEXED, INDEXED_CONSTANT, RANDOM = Value
  }

  trait NaiveIntegerDistribution {
    def sample(param: Symbol): Int
  }

  class ConstantDistribution(constant: Int) extends NaiveIntegerDistribution {
    override def sample(param: Symbol): Int = constant
  }

  class CounterDistribution(counter: AtomicInteger) extends NaiveIntegerDistribution {
    override def sample(param: Symbol): Int = counter.getAndIncrement()
  }

  class RandomDistribution(seed: Int) extends NaiveIntegerDistribution {
    private val random = new Random(seed)

    override def sample(param: Symbol): Int = random.nextInt()
  }

  class HashedDistribution(salt: Int) extends NaiveIntegerDistribution {
    private val messageDigest = MessageDigest.getInstance("SHA-1")

    override def sample(param: Symbol): Int = {
      val paramName = param.name.decodedName
      val saltedParamName = s"$paramName.$salt"
      val digest = messageDigest.digest(saltedParamName.getBytes(StandardCharsets.UTF_8))
      ByteBuffer.wrap(digest).getInt
    }
  }

  def constant[T <: Product : TypeTag](value: Int): T = {
    make[T](new ConstantDistribution(value))
  }

  def empty[T <: Product : TypeTag]: T = {
    val args = Reflection.getCaseClassFields[T].map { param =>
      param.typeSignature.dealias match {
        case t if t <:< typeOf[Option[_]] => Option.empty
        case t if t <:< typeOf[Set[_]] => Set.empty
        case t if t <:< typeOf[Seq[_]] => Seq.empty
        case t if t <:< typeOf[Map[_, _]] => Map.empty
        case t if t <:< typeOf[Iterable[_]] => Iterable.empty

        case t if t <:< typeOf[Enumeration#Value] => chooseEnumValue(0)(Reflection.typeToTypeTag(t))

        case t if t =:= typeOf[Boolean] => false
        case t if t =:= typeOf[Byte] => 0.toByte
        case t if t =:= typeOf[Short] => 0.toShort
        case t if t =:= typeOf[Char] => 0.toChar
        case t if t =:= typeOf[Int] => 0
        case t if t =:= typeOf[Long] => 0.toLong
        case t if t =:= typeOf[Float] => 0.toFloat
        case t if t =:= typeOf[Double] => 0.toDouble

        case t if Reflection.isCaseClass(t) => empty(Reflection.typeToTypeTag(t))
        case _ => null
      }
    }
    Reflection.createCaseClassInstance[T](args)
  }

  def indexed[T <: Product : TypeTag](from: Int): T = {
    val counter = new AtomicInteger(from)
    make[T](new CounterDistribution(counter))
  }

  def random[T <: Product : TypeTag](seed: Int): T = {
    make[T](new RandomDistribution(seed))
  }

  def hashed[T <: Product : TypeTag](salt: Int): T = {
    make[T](new HashedDistribution(salt))
  }

  def make[T <: Product : TypeTag](distribution: NaiveIntegerDistribution): T = {
    val args = Reflection.getCaseClassFields[T].map { param =>
      val number = distribution.sample(param)
      param.typeSignature.dealias match {
        case t if t <:< typeOf[Option[_]] => Option.empty
        case t if t <:< typeOf[Set[_]] => Set.empty
        case t if t <:< typeOf[Seq[_]] => Seq.empty
        case t if t <:< typeOf[Map[_, _]] => Map.empty
        case t if t <:< typeOf[Iterable[_]] => Iterable.empty

        case t if t <:< typeOf[Enumeration#Value] => chooseEnumValue(number)(Reflection.typeToTypeTag(t))

        case t if t =:= typeOf[Boolean] => (number & 1) == 1
        case t if t =:= typeOf[Byte] => number.toByte
        case t if t =:= typeOf[Short] => number.toShort
        case t if t =:= typeOf[Char] => number.toChar
        case t if t =:= typeOf[Int] => number.toInt
        case t if t =:= typeOf[Long] => number.toLong
        case t if t =:= typeOf[Float] => number.toFloat
        case t if t =:= typeOf[Double] => number.toDouble
        case t if t =:= typeOf[String] => number.toString

        case t if t =:= typeOf[LocalDate] => makeLocalDate(number)
        case t if t =:= typeOf[LocalTime] => makeLocalTime(number)
        case t if t =:= typeOf[LocalDateTime] => makeLocalDateTime(number)
        case t if t =:= typeOf[Instant] => makeLocalDateTime(number).toInstant(ZoneOffset.UTC)
        case t if t =:= typeOf[java.sql.Date] => java.sql.Date.valueOf(makeLocalDate(number))
        case t if t =:= typeOf[java.sql.Timestamp] => java.sql.Timestamp.valueOf(makeLocalDateTime(number))

        case t if Reflection.isCaseClass(t) => make(distribution)(Reflection.typeToTypeTag(t))
        case t => throw new IllegalArgumentException(s"Generating type '$t' is not supported.")
      }
    }
    Reflection.createCaseClassInstance[T](args)
  }

  private def chooseEnumValue[E <: Enumeration : TypeTag](index: Int): E#Value = {
    val values = Reflection.getEnumerationValues[E]
    values(Math.abs(index) % values.size)
  }

  private def makeLocalDate(day: Int): LocalDate = {
    val minDay = LocalDate.of(0, 1, 1).toEpochDay
    val maxDay = LocalDate.of(9999, 12, 31).toEpochDay
    val adjustedDay = Math.abs(day - minDay) % maxDay + minDay
    LocalDate.ofEpochDay(adjustedDay)
  }

  private def makeLocalTime(second: Int): LocalTime = {
    val minSecond = LocalTime.of(0, 0, 0).toSecondOfDay
    val maxSecond = LocalTime.of(23, 59, 59).toSecondOfDay
    val adjustedSecond = Math.abs(second - minSecond) % maxSecond + minSecond
    LocalTime.ofSecondOfDay(adjustedSecond)
  }

  private def makeLocalDateTime(number: Int): LocalDateTime = {
    LocalDateTime.of(makeLocalDate(number), makeLocalTime(number))
  }
}

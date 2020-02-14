package com.nuna.trustdb.core.util

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time._
import java.time.temporal.Temporal
import java.util.concurrent.atomic.AtomicLong

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
class Arbitrary(strategy: Arbitrary.ArbitraryStrategy.Value, seed: Long = 0) {
  import Arbitrary._

  private val counter = new AtomicLong(seed)

  def make[T <: Product : TypeTag]: T = {
    strategy match {
      case ArbitraryStrategy.CONSTANT => Arbitrary.constant[T](seed)
      case ArbitraryStrategy.DEFAULT => Arbitrary.default[T]
      case ArbitraryStrategy.EMPTY => Arbitrary.empty[T]
      case ArbitraryStrategy.HASHED => Arbitrary.hashed[T](counter.getAndIncrement())
      case ArbitraryStrategy.HASHED_CONSTANT => Arbitrary.hashed[T](seed)
      case ArbitraryStrategy.INDEXED => Arbitrary.indexed[T](counter.getAndAdd(1000))
      case ArbitraryStrategy.INDEXED_CONSTANT => Arbitrary.constant[T](counter.getAndIncrement())
      case ArbitraryStrategy.PROTO => Arbitrary.proto[T]
      case ArbitraryStrategy.RANDOM => Arbitrary.random[T](counter.getAndIncrement())
    }
  }
}

object Arbitrary {
  object ArbitraryStrategy extends Enumeration {
    val CONSTANT, DEFAULT, EMPTY, HASHED, HASHED_CONSTANT, INDEXED, INDEXED_CONSTANT, PROTO, RANDOM = Value
  }

  private val EPOCH_DAY_MIN = LocalDate.of(1, 1, 1).toEpochDay
  private val EPOCH_DAY_MAX = LocalDate.of(9999, 12, 31).toEpochDay
  private val SECOND_OF_DAY_MIN = LocalTime.of(0, 0, 0).toSecondOfDay
  private val SECOND_OF_DAY_MAX = LocalTime.of(23, 59, 59).toSecondOfDay

  trait NaiveDistribution {
    def sample(param: Symbol): Long
  }

  class ConstantDistribution(constant: Long) extends NaiveDistribution {
    override def sample(param: Symbol): Long = constant
  }

  class CounterDistribution(counter: AtomicLong) extends NaiveDistribution {
    override def sample(param: Symbol): Long = counter.getAndIncrement()
  }

  class RandomDistribution(seed: Long) extends NaiveDistribution {
    private val random = new Random(seed)

    override def sample(param: Symbol): Long = random.nextLong()
  }

  class HashedDistribution(salt: Long) extends NaiveDistribution {
    private val messageDigest = MessageDigest.getInstance("SHA-1")

    override def sample(param: Symbol): Long = {
      val paramName = param.name.decodedName
      val saltedParamName = s"$paramName.$salt"
      val digest = messageDigest.digest(saltedParamName.getBytes(StandardCharsets.UTF_8))
      ByteBuffer.wrap(digest).getLong
    }
  }

  def constant[T <: Product : TypeTag](value: Long): T = {
    make[T](new ConstantDistribution(value))
  }

  def default[T <: Product : TypeTag]: T = {
    val args = Reflection.getCaseClassFields[T].map { param =>
      param.typeSignature.dealias match {
        case t if t <:< typeOf[AnyVal] => makePrimitive(t, 0)
        case t if Reflection.isCaseClass(t) => default(Reflection.typeToTypeTag(t))
        case _ => null
      }
    }
    Reflection.createCaseClassInstance[T](args)
  }

  def empty[T <: Product : TypeTag]: T = {
    val args = Reflection.getCaseClassFields[T].map { param =>
      param.typeSignature.dealias match {
        case t if t <:< typeOf[AnyVal] => makePrimitive(t, 0)
        case t if t =:= typeOf[String] => null
        case t if t <:< typeOf[Enumeration#Value] => makeEnumerationValue(0)(Reflection.typeToTypeTag(t))
        case t if t <:< typeOf[Option[_]] => Option.empty
        case t if t <:< typeOf[Iterable[_]] => makeEmptyCollection(t)
        case t if t <:< typeOf[Temporal] || t <:< typeOf[java.util.Date] => null
        case t if Reflection.isCaseClass(t) => empty(Reflection.typeToTypeTag(t))
        case _ => null
      }
    }
    Reflection.createCaseClassInstance[T](args)
  }

  def hashed[T <: Product : TypeTag](salt: Long): T = {
    make[T](new HashedDistribution(salt))
  }

  def indexed[T <: Product : TypeTag](from: Long): T = {
    val counter = new AtomicLong(from)
    make[T](new CounterDistribution(counter))
  }

  def proto[T <: Product : TypeTag]: T = {
    val args = Reflection.getCaseClassFields[T].map { param =>
      param.typeSignature.dealias match {
        case t if t <:< typeOf[AnyVal] => makePrimitive(t, 0)
        case t if t =:= typeOf[String] => ""
        case t if t <:< typeOf[Enumeration#Value] => makeEnumerationValue(0)(Reflection.typeToTypeTag(t))
        case t if t <:< typeOf[Option[_]] => Option.empty
        case t if t <:< typeOf[Iterable[_]] => makeEmptyCollection(t)
        case t if t <:< typeOf[Temporal] || t <:< typeOf[java.util.Date] => null
        case t if Reflection.isCaseClass(t) => proto(Reflection.typeToTypeTag(t))
        case _ => null
      }
    }
    Reflection.createCaseClassInstance[T](args)
  }

  def random[T <: Product : TypeTag](seed: Long): T = {
    make[T](new RandomDistribution(seed))
  }

  def make[T <: Product : TypeTag](distribution: NaiveDistribution): T = {
    val args = Reflection.getCaseClassFields[T].map { param =>
      val number = distribution.sample(param)
      param.typeSignature.dealias match {
        case t if t <:< typeOf[AnyVal] => makePrimitive(t, number)
        case t if t =:= typeOf[String] => number.toString
        case t if t <:< typeOf[Enumeration#Value] => makeEnumerationValue(number)(Reflection.typeToTypeTag(t))
        case t if t <:< typeOf[Option[_]] => Option.empty
        case t if t <:< typeOf[Iterable[_]] => makeEmptyCollection(t)
        case t if t <:< typeOf[Temporal] || t <:< typeOf[java.util.Date] => makeTemporal(t, number)
        case t if Reflection.isCaseClass(t) => make(distribution)(Reflection.typeToTypeTag(t))
        case t => throw new IllegalArgumentException(s"Generating type '$t' is not supported.")
      }
    }
    Reflection.createCaseClassInstance[T](args)
  }

  private def makePrimitive(typ: Type, number: Long): AnyVal = {
    typ match {
      case t if t =:= typeOf[Boolean] => (number & 1) == 1
      case t if t =:= typeOf[Byte] => number.toByte
      case t if t =:= typeOf[Short] => number.toShort
      case t if t =:= typeOf[Char] => number.toChar
      case t if t =:= typeOf[Int] => number.toInt
      case t if t =:= typeOf[Long] => number.toLong
      case t if t =:= typeOf[Float] => number.toFloat
      case t if t =:= typeOf[Double] => number.toDouble
      case t => throw new IllegalArgumentException(s"Generating primitive of type '$t' is not supported.")
    }
  }

  private def makeEnumerationValue[E <: Enumeration : TypeTag](number: Long): E#Value = {
    val values = Reflection.getEnumerationValues[E]
    values(clamp(number, 0, values.size).toInt)
  }

  private def makeEmptyCollection(typ: Type): Any = {
    typ match {
      case t if t <:< typeOf[Set[_]] => Set.empty
      case t if t <:< typeOf[Seq[_]] => Seq.empty
      case t if t <:< typeOf[Map[_, _]] => Map.empty
      case t if t <:< typeOf[Iterable[_]] => Iterable.empty
      case t => throw new IllegalArgumentException(s"Generating collection of type '$t' is not supported.")
    }
  }

  private def makeTemporal(typ: Type, number: Long): Any = {
    typ match {
      case t if t =:= typeOf[java.util.Date] =>
        java.util.Date.from(java.sql.Date.valueOf(makeLocalDate(number)).toInstant)
      case t if t =:= typeOf[java.sql.Date] => java.sql.Date.valueOf(makeLocalDate(number))
      case t if t =:= typeOf[java.sql.Timestamp] => java.sql.Timestamp.valueOf(makeLocalDateTime(number))
      case t if t =:= typeOf[LocalDate] => makeLocalDate(number)
      case t if t =:= typeOf[LocalTime] => makeLocalTime(number)
      case t if t =:= typeOf[LocalDateTime] => makeLocalDateTime(number)
      case t if t =:= typeOf[Instant] => makeLocalDateTime(number).toInstant(ZoneOffset.UTC)
      case t => throw new IllegalArgumentException(s"Generating temporal of type '$t' is not supported.")
    }
  }

  private def makeLocalDate(day: Long): LocalDate = {
    LocalDate.ofEpochDay(clamp(day, EPOCH_DAY_MIN, EPOCH_DAY_MAX))
  }

  private def makeLocalTime(second: Long): LocalTime = {
    LocalTime.ofSecondOfDay(clamp(second, SECOND_OF_DAY_MIN, SECOND_OF_DAY_MAX))
  }

  private def makeLocalDateTime(number: Long): LocalDateTime = {
    LocalDateTime.of(makeLocalDate(number), makeLocalTime(number))
  }

  private[util] def clamp(value: Long, min: Long, max: Long): Long = {
    val span = max - min + 1
    val mod = (value - min) % span
    val adjustedMod = if (mod < 0) mod + span else mod
    adjustedMod + min
  }
}

package io.stoys.scala

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time._
import java.time.temporal.Temporal
import java.util.Date
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
class Arbitrary(strategy: ArbitraryStrategy, seed: Long = 0) {
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
  import Reflection._

  private val EPOCH_DAY_MIN = LocalDate.of(1, 1, 1).toEpochDay
  private val EPOCH_DAY_MAX = LocalDate.of(9999, 12, 31).toEpochDay
  private val SECOND_OF_DAY_MIN = LocalTime.of(0, 0, 0).toSecondOfDay
  private val SECOND_OF_DAY_MAX = LocalTime.of(23, 59, 59).toSecondOfDay

  private trait NaiveDistribution {
    def sample(param: Symbol): Long
  }

  private class ConstantDistribution(constant: Long) extends NaiveDistribution {
    override def sample(param: Symbol): Long = constant
  }

  private class CounterDistribution(counter: AtomicLong) extends NaiveDistribution {
    override def sample(param: Symbol): Long = counter.getAndIncrement()
  }

  private class RandomDistribution(seed: Long) extends NaiveDistribution {
    private val random = new Random(seed)

    override def sample(param: Symbol): Long = random.nextLong()
  }

  private class HashedDistribution(salt: Long) extends NaiveDistribution {
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

  private def default(tpe: Type): Any = {
    val args = getCaseClassFields(tpe).map { param =>
      baseType(param.typeSignature) match {
        case t if isSubtype(t, typeOf[AnyVal]) => makePrimitive(t, 0)
        case t if isCaseClass(t) => default(t)
        case _ => null
      }
    }
    createCaseClassInstance(tpe, args)
  }

  def default[T <: Product : TypeTag]: T = cleanupReflection {
    default(localTypeOf[T]).asInstanceOf[T]
  }

  private def empty(tpe: Type): Any = {
    val args = getCaseClassFields(tpe).map { param =>
      baseType(param.typeSignature) match {
        case t if isSubtype(t, definitions.AnyValTpe) => makePrimitive(t, 0)
        case t if isSubtype(t, localTypeOf[String]) => null
        case t if isSubtype(t, localTypeOf[Enumeration#Value]) => makeEnumerationValue(t, 0)
        case t if isSubtype(t, localTypeOf[Option[_]]) => Option.empty
        case t if isSubtype(t, localTypeOf[Iterable[_]]) => makeEmptyCollection(t)
        case t if isSubtype(t, localTypeOf[Temporal]) || isSubtype(t, localTypeOf[Date]) => null
        case t if isCaseClass(t) => empty(t)
        case _ => null
      }
    }
    createCaseClassInstance(tpe, args)
  }

  def empty[T <: Product : TypeTag]: T = cleanupReflection {
    empty(localTypeOf[T]).asInstanceOf[T]
  }

  def hashed[T <: Product : TypeTag](salt: Long): T = {
    make[T](new HashedDistribution(salt))
  }

  def indexed[T <: Product : TypeTag](from: Long): T = {
    val counter = new AtomicLong(from)
    make[T](new CounterDistribution(counter))
  }

  private def proto(tpe: Type): Any = {
    val args = getCaseClassFields(tpe).map { param =>
      baseType(param.typeSignature) match {
        case t if isSubtype(t, definitions.AnyValTpe) => makePrimitive(t, 0)
        case t if isSubtype(t, localTypeOf[String]) => ""
        case t if isSubtype(t, localTypeOf[Enumeration#Value]) => makeEnumerationValue(t, 0)
        case t if isSubtype(t, localTypeOf[Option[_]]) => Option.empty
        case t if isSubtype(t, localTypeOf[Iterable[_]]) => makeEmptyCollection(t)
        case t if isSubtype(t, localTypeOf[Temporal]) || isSubtype(t, localTypeOf[Date]) => null
        case t if isCaseClass(t) => proto(t)
        case _ => null
      }
    }
    createCaseClassInstance(tpe, args)
  }

  def proto[T <: Product : TypeTag]: T = cleanupReflection {
    proto(localTypeOf[T]).asInstanceOf[T]
  }

  def random[T <: Product : TypeTag](seed: Long): T = {
    make[T](new RandomDistribution(seed))
  }

  private def make(tpe: Type, distribution: NaiveDistribution): Any = {
    val args = getCaseClassFields(tpe).map { param =>
      val number = distribution.sample(param)
      baseType(param.typeSignature) match {
        case t if isSubtype(t, definitions.AnyValTpe) => makePrimitive(t, number)
        case t if isSubtype(t, localTypeOf[String]) => number.toString
        case t if isSubtype(t, localTypeOf[Enumeration#Value]) => makeEnumerationValue(t, number)
        case t if isSubtype(t, localTypeOf[Option[_]]) => Option.empty
        case t if isSubtype(t, localTypeOf[Iterable[_]]) => makeEmptyCollection(t)
        case t if isSubtype(t, localTypeOf[Temporal]) || isSubtype(t, localTypeOf[Date]) => makeTemporal(t, number)
        case t if isCaseClass(t) => make(t, distribution)
        case t => throw new IllegalArgumentException(s"Generating type '$t' is not supported.")
      }
    }
    createCaseClassInstance(tpe, args)
  }

  def make[T <: Product : TypeTag](distribution: NaiveDistribution): T = cleanupReflection {
    make(localTypeOf[T], distribution).asInstanceOf[T]
  }

  private def makePrimitive(tpe: Type, number: Long): AnyVal = {
    tpe match {
      case t if t =:= definitions.BooleanTpe => (number & 1) == 1
      case t if t =:= definitions.ByteTpe => number.toByte
      case t if t =:= definitions.ShortTpe => number.toShort
      case t if t =:= definitions.CharTpe => number.toChar
      case t if t =:= definitions.IntTpe => number.toInt
      case t if t =:= definitions.LongTpe => number.toLong
      case t if t =:= definitions.FloatTpe => number.toFloat
      case t if t =:= definitions.DoubleTpe => number.toDouble
      case t => throw new IllegalArgumentException(s"Generating primitive of type '$t' is not supported.")
    }
  }

  private def makeEnumerationValue(tpe: Type, number: Long): Enumeration#Value = {
    val values = enumerationValuesOf(tpe)
    values(clamp(number, 0, values.size).toInt)
  }

  private def makeEmptyCollection(tpe: Type): Any = {
    tpe match {
      case t if isSubtype(t, localTypeOf[Set[_]]) => Set.empty
      case t if isSubtype(t, localTypeOf[Seq[_]]) => Seq.empty
      case t if isSubtype(t, localTypeOf[Map[_, _]]) => Map.empty
      case t if isSubtype(t, localTypeOf[Iterable[_]]) => Iterable.empty
      case t => throw new IllegalArgumentException(s"Generating collection of type '$t' is not supported.")
    }
  }

  private def makeTemporal(tpe: Type, number: Long): Any = {
    tpe match {
      case t if t =:= localTypeOf[Date] => Date.from(java.sql.Date.valueOf(makeLocalDate(number)).toInstant)
      case t if t =:= localTypeOf[java.sql.Date] => java.sql.Date.valueOf(makeLocalDate(number))
      case t if t =:= localTypeOf[java.sql.Timestamp] => java.sql.Timestamp.valueOf(makeLocalDateTime(number))
      case t if t =:= localTypeOf[LocalDate] => makeLocalDate(number)
      case t if t =:= localTypeOf[LocalTime] => makeLocalTime(number)
      case t if t =:= localTypeOf[LocalDateTime] => makeLocalDateTime(number)
      case t if t =:= localTypeOf[Instant] => makeLocalDateTime(number).toInstant(ZoneOffset.UTC)
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

  private[scala] def clamp(value: Long, min: Long, max: Long): Long = {
    val span = max - min + 1
    val mod = (value - min) % span
    val adjustedMod = if (mod < 0) mod + span else mod
    adjustedMod + min
  }
}

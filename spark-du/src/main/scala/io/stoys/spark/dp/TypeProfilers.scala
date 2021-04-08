package io.stoys.spark.dp

import io.stoys.spark.dp.sketches.{CardinalitySketch, FrequencySketch, QuantileSketch}
import org.apache.spark.sql.types._

import java.lang.{Long => JLong}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

private[dp] trait TypeProfiler[T] {
  def update(value: Any): Unit

  def merge(that: TypeProfiler[T]): TypeProfiler[T]

  def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn]
}

private[dp] class AnyProfiler(
    val name: String,
    val simpleDataTypeName: String,
    val nullable: Boolean,
    var count: Long,
    var countNulls: Long,
    var cardinalitySketch: CardinalitySketch,
    var frequencySketch: FrequencySketch[Any],
    var typeProfiler: TypeProfiler[Any]
) extends TypeProfiler[Any] {

  override def update(value: Any): Unit = {
    count += 1
    if (value == null) {
      countNulls += 1
    } else {
      if (simpleDataTypeName != null) {
        cardinalitySketch.update(value)
        frequencySketch.update(value)
      }
      typeProfiler.update(value)
    }
  }

  override def merge(that: TypeProfiler[Any]): AnyProfiler = {
    that match {
      case that: AnyProfiler if this.count == 0L => that
      case that: AnyProfiler if that.count == 0L => this
      case that: AnyProfiler =>
        this.count += that.count
        this.countNulls += that.countNulls
        if (simpleDataTypeName != null) {
          this.cardinalitySketch.merge(that.cardinalitySketch)
          this.frequencySketch.merge(that.frequencySketch)
        }
        this.typeProfiler.merge(that.typeProfiler)
        this
    }
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = {
    if (count == 0L) {
      None
    } else {
      val baseProfile = DpColumn(
        name = name,
        data_type = simpleDataTypeName,
        nullable = nullable,
        enum_values = Seq.empty,
        format = None,
        count = count,
        count_empty = None,
        count_nulls = Some(countNulls),
        count_unique = Option(cardinalitySketch).flatMap(_.getNumberOfUnique),
        count_zeros = None,
        max_length = None,
        min = None,
        max = None,
        mean = None,
        pmf = Seq.empty,
        items = Option(frequencySketch).toSeq.flatMap(_.getItems(_.toString).sortBy(_.item)),
        extras = Map.empty
      )
      typeProfiler.profile(Some(baseProfile), config)
    }
  }
}

private[dp] object AnyProfiler {
  private def getTypeProfiler(dataType: DataType, config: DpConfig): TypeProfiler[Any] = {
    val typeProfiler = dataType match {
      case _: BooleanType | ByteType | ShortType | IntegerType | LongType | DateType | TimestampType =>
        IntegralProfiler.zero(dataType, config)
      case _: FloatType | DoubleType | _: DecimalType => FractionalProfiler.zero(dataType, config)
      case _: StringType if config.infer_types_from_strings => TypeInferenceStringProfiler.zero(config)
      case _: StringType => StringProfiler.zero(config)
      case _: ArrayType | BinaryType | _: MapType => IterableProfiler.zero
      case _: StructType => new StructProfiler()
    }
    typeProfiler.asInstanceOf[TypeProfiler[Any]]
  }

  def zero(field: StructField, config: DpConfig): AnyProfiler = {
    field.dataType match {
      case _: ArrayType | _: MapType | _: StructType =>
        new AnyProfiler(
          name = field.name,
          simpleDataTypeName = null,
          nullable = field.nullable,
          count = 0L,
          countNulls = 0L,
          cardinalitySketch = null,
          frequencySketch = null,
          typeProfiler = getTypeProfiler(field.dataType, config)
        )
      case _ =>
        new AnyProfiler(
          name = field.name,
          simpleDataTypeName = field.dataType.typeName,
          nullable = field.nullable,
          count = 0L,
          countNulls = 0L,
          cardinalitySketch = CardinalitySketch.create(config),
          frequencySketch = FrequencySketch.create(config),
          typeProfiler = getTypeProfiler(field.dataType, config)
        )
    }
  }
}

private[dp] class StringProfiler(
    var countEmpty: Long,
    var min: String,
    var max: String,
    var maxLength: Int,
    val maxItemLength: Int
) extends TypeProfiler[String] {

  def updateString(value: String): Unit = {
    if (value != null) {
      if (value.isEmpty) {
        countEmpty += 1
      }
      val valueTruncated = value.substring(0, math.min(value.length, maxItemLength))
      if (min == null || min > valueTruncated) {
        min = valueTruncated
      }
      if (max == null || max < valueTruncated) {
        max = valueTruncated
      }
      if (maxLength < value.length) {
        maxLength = value.length
      }
    }
  }

  override def update(value: Any): Unit = {
    value match {
      case s: String => updateString(s)
    }
  }

  override def merge(that: TypeProfiler[String]): StringProfiler = {
    that match {
      case that: StringProfiler =>
        this.countEmpty += that.countEmpty
        if (this.min == null || (that.min != null && this.min > that.min)) {
          this.min = that.min
        }
        if (this.max == null || (that.max != null && this.max < that.max)) {
          this.max = that.max
        }
        this.maxLength = math.max(this.maxLength, that.maxLength)
    }
    this
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = {
    baseProfile.map { profile =>
      profile.count - profile.count_nulls.getOrElse(0L) match {
        case 0L => profile
        case _ =>
          profile.copy(
            min = Option(min),
            max = Option(max),
            count_empty = Some(countEmpty),
            max_length = Some(maxLength)
          )
      }
    }
  }
}

private[dp] object StringProfiler {
  def zero(config: DpConfig): StringProfiler = {
    new StringProfiler(
      countEmpty = 0L,
      min = null,
      max = null,
      maxLength = 0,
      maxItemLength = config.max_item_length
    )
  }
}

private[dp] class IntegralProfiler(
    var countZero: Long,
    var min: Long,
    var max: Long,
    var sum: Long,
    var quantileSketch: QuantileSketch,
    val zoneId: ZoneId
) extends TypeProfiler[Long] {

  def updateLong(value: Long): Unit = {
    if (value == 0L) {
      countZero += 1
    }
    min = math.min(min, value)
    max = math.max(max, value)
    sum += value
    quantileSketch.update(value)
  }

  override def update(value: Any): Unit = {
    value match {
      case b: Boolean => updateLong(if (b) 1 else 0)
      case b: Byte => updateLong(b)
      case s: Short => updateLong(s)
      case i: Int => updateLong(i)
      case l: Long => updateLong(l)
      case d: Date => updateLong(d.getTime / 1000L)
      case t: Timestamp => updateLong(t.getTime / 1000L)
      case ld: LocalDate => updateLong(ld.atStartOfDay(zoneId).toEpochSecond)
      case ldt: LocalDateTime => updateLong(ldt.atZone(zoneId).toEpochSecond)
      case i: Instant => updateLong(i.getEpochSecond)
    }
  }

  override def merge(that: TypeProfiler[Long]): IntegralProfiler = {
    that match {
      case that: IntegralProfiler =>
        this.countZero += that.countZero
        this.min = math.min(this.min, that.min)
        this.max = math.max(this.max, that.max)
        this.sum = this.sum + that.sum
        this.quantileSketch.merge(that.quantileSketch)
    }
    this
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = {
    profile(baseProfile, _.toString)
  }

  def profile(baseProfile: Option[DpColumn], itemToString: Long => String): Option[DpColumn] = {
    baseProfile.map { profile =>
      profile.count - profile.count_nulls.getOrElse(0L) match {
        case 0L => profile
        case countNonNulls =>
          profile.copy(
            min = Some(itemToString(min)),
            max = Some(itemToString(max)),
            mean = Some(math.round(sum.toDouble / countNonNulls)),
            count_zeros = Some(countZero),
            max_length = Some((maxLengthBits + 7) / 8),
            pmf = quantileSketch.getPmfBuckets,
            extras = Map(
              "max_length_bits" -> maxLengthBits.toString,
              "quantiles" -> quantileSketch.getQuantiles(Seq(0.25, 0.5, 0.75)).mkString("[", ",", "]")
            )
          )
      }
    }
  }

  def toFractionalProfiler: FractionalProfiler = {
    new FractionalProfiler(
      countNaN = 0L,
      countZero = countZero,
      min = min,
      max = max,
      sum = sum,
      quantileSketch = quantileSketch,
      requiresDoublePrecision = maxLengthBits >= 24
    )
  }

  private def maxLengthBits: Int = {
    val minBits = JLong.SIZE - JLong.numberOfLeadingZeros(math.abs(min))
    val maxBits = JLong.SIZE - JLong.numberOfLeadingZeros(math.abs(max))
    math.max(minBits, maxBits)
  }
}

private[dp] object IntegralProfiler {
  def zero(dataType: DataType, config: DpConfig): IntegralProfiler = {
    new IntegralProfiler(
      countZero = 0L,
      min = Long.MaxValue,
      max = Long.MinValue,
      sum = 0L,
      quantileSketch = QuantileSketch.create(config),
      zoneId = ZoneId.of(config.time_zone_id.getOrElse(Dp.DEFAULT_TIME_ZONE_ID))
    )
  }
}

private[dp] class FractionalProfiler(
    var countNaN: Long,
    var countZero: Long,
    var min: Double,
    var max: Double,
    var sum: Double,
    var quantileSketch: QuantileSketch,
    var requiresDoublePrecision: Boolean
) extends TypeProfiler[Double] {

  def updateDouble(value: Double): Unit = {
    if (value.isNaN) {
      countNaN += 1
    } else {
      if (value == 0.0) {
        countZero += 1
      }
      min = math.min(min, value)
      max = math.max(max, value)
      sum = sum + value
      quantileSketch.update(value)
    }
  }

  override def update(value: Any): Unit = {
    value match {
      case f: Float => updateDouble(f)
      case d: Double => updateDouble(d)
      case bd: BigDecimal => updateDouble(bd.toDouble)
    }
  }

  override def merge(that: TypeProfiler[Double]): FractionalProfiler = {
    that match {
      case that: FractionalProfiler =>
        this.countNaN += that.countNaN
        this.countZero += that.countZero
        this.min = math.min(this.min, that.min)
        this.max = math.max(this.max, that.max)
        this.sum = this.sum + that.sum
        this.quantileSketch.merge(that.quantileSketch)
        this.requiresDoublePrecision |= that.requiresDoublePrecision
    }
    this
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = {
    profile(baseProfile, _.toString)
  }

  def profile(baseProfile: Option[DpColumn], itemToString: Double => String): Option[DpColumn] = {
    baseProfile.map { profile =>
      profile.count - profile.count_nulls.getOrElse(0L) match {
        case 0L => profile
        case countNonNulls =>
          val maxLength = profile.data_type match {
            case "float" | "string" if !requiresDoublePrecision => Some(4L)
            case "float" | "double" | "string" => Some(8L)
//            case "decimal" | "string" => None // TODO: support decimals
          }
          profile.copy(
            min = Some(itemToString(min)),
            max = Some(itemToString(max)),
            mean = Some(sum / (countNonNulls - countNaN)),
            count_empty = Some(countNaN),
            count_zeros = Some(countZero),
            max_length = maxLength,
            pmf = quantileSketch.getPmfBuckets,
            extras = Map(
              "quantiles" -> quantileSketch.getQuantiles(Seq(0.25, 0.5, 0.75)).mkString("[", ",", "]")
            )
          )
      }
    }
  }
}

private[dp] object FractionalProfiler {
  def zero(dataType: DataType, config: DpConfig): FractionalProfiler = {
    new FractionalProfiler(
      countNaN = 0L,
      countZero = 0L,
      min = Double.MaxValue,
      max = Double.MinValue,
      sum = 0.0,
      quantileSketch = QuantileSketch.create(config),
      requiresDoublePrecision = dataType == DoubleType
    )
  }
}

private[dp] class IterableProfiler(
    var countEmpty: Long,
    var minLength: Int,
    var maxLength: Int
) extends TypeProfiler[Iterable[_]] {

  def updateIterable(value: Iterable[_]): Unit = {
    if (value.isEmpty) {
      countEmpty += 1
    }
    if (minLength > value.size) {
      minLength = value.size
    }
    if (maxLength < value.size) {
      maxLength = value.size
    }
  }

  override def update(value: Any): Unit = {
    value match {
      case s: Seq[_] => updateIterable(s)
      case a: Array[_] => updateIterable(a)
      case m: Map[_, _] => updateIterable(m)
    }
  }

  override def merge(that: TypeProfiler[Iterable[_]]): IterableProfiler = {
    that match {
      case that: IterableProfiler =>
        this.countEmpty += that.countEmpty
        this.minLength = math.min(this.minLength, that.minLength)
        this.maxLength = math.max(this.maxLength, that.maxLength)
    }
    this
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = {
    baseProfile.map { profile =>
      profile.count - profile.count_nulls.getOrElse(0L) match {
        case 0L => profile
        case _ =>
          profile.copy(
            count_empty = Some(countEmpty),
            max_length = Some(maxLength)
          )
      }
    }
  }
}

private[dp] object IterableProfiler {
  def zero: IterableProfiler = {
    new IterableProfiler(
      countEmpty = 0L,
      minLength = 0,
      maxLength = 0
    )
  }
}

private[dp] class StructProfiler() extends TypeProfiler[AnyRef] {
  override def update(value: Any): Unit = Unit

  override def merge(that: TypeProfiler[AnyRef]): StructProfiler = this

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = baseProfile
}

private[dp] object StructProfiler {
  def zero: StructProfiler = {
    new StructProfiler()
  }
}

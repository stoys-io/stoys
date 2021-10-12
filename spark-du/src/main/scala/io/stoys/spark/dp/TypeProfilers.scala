package io.stoys.spark.dp

import io.stoys.spark.dp.sketches.{CardinalitySketch, FrequencySketch, QuantileSketch}
import org.apache.spark.sql.types._

import java.lang.{Long => JLong}
import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import scala.collection.compat.IterableOnce
import scala.util.Try

private[dp] trait TypeProfiler[T] {
  def update(value: Any): Double

  def merge(that: TypeProfiler[T]): TypeProfiler[T]

  def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn]
}

private[dp] object TypeProfiler {
  val IS_EXACT_EXTRAS_KEY = "is_exact"
  val IS_INFERRED_TYPE_EXTRAS_KEY = "is_inferred_type"
  val MAX_LENGTH_IN_BITS_EXTRAS_KEY = "max_length_in_bits"
  val QUANTILES_EXTRAS_KEY = "quantiles"
  val ZONE_ID_EXTRAS_KEY = "zone_id"
}

private[dp] case class AnyProfiler(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    var count: Long,
    var countNulls: Long,
    var typeProfiler: TypeProfiler[Any],
    var cardinalitySketch: CardinalitySketch,
    var frequencySketch: FrequencySketch[Any],
    var quantileSketch: QuantileSketch,
) extends TypeProfiler[Any] {

  override def update(value: Any): Double = {
    count += 1
    if (value == null) {
      countNulls += 1
      Double.NaN
    } else {
      val doubleValue = typeProfiler.update(value)
      if (dataType != null) {
        cardinalitySketch.update(value)
        frequencySketch.update(value)
        quantileSketch.update(doubleValue)
      }
      doubleValue
    }
  }

  override def merge(that: TypeProfiler[Any]): AnyProfiler = {
    that match {
      case that: AnyProfiler if this.count == 0L => that
      case that: AnyProfiler if that.count == 0L => this
      case that: AnyProfiler =>
        this.count += that.count
        this.countNulls += that.countNulls
        this.typeProfiler.merge(that.typeProfiler)
        if (dataType != null) {
          this.cardinalitySketch.merge(that.cardinalitySketch)
          this.frequencySketch.merge(that.frequencySketch)
          this.quantileSketch.merge(that.quantileSketch)
        }
        this
    }
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = {
    if (count == 0L) {
      None
    } else {
      val isExact = Option(frequencySketch).map(_.isExact).filter(identity)
      val items = Option(frequencySketch).map(_.getItems(_.toString)).getOrElse(Seq.empty).sortBy(_.item)
      val exactUnique = isExact.map(_ => items.size.toLong)
      val approximateUnique = Option(cardinalitySketch).flatMap(_.getNumberOfUnique)
      val baseProfile = DpColumn(
        name = name,
        data_type = Option(dataType).map(_.typeName).orNull,
        data_type_json = Option(dataType).map(_.json).orNull,
        nullable = nullable,
        enum_values = Seq.empty,
        format = None,
        count = count,
        count_empty = None,
        count_nulls = Some(countNulls),
        count_unique = exactUnique.orElse(approximateUnique),
        count_zeros = None,
        max_length = None,
        min = None,
        max = None,
        mean = None,
        pmf = Option(quantileSketch).map(_.getPmfBuckets).getOrElse(Seq.empty),
        items = items,
        extras = getExactExtras ++ getQuantileExtras,
      )
      val profile = typeProfiler.profile(Some(baseProfile), config)
      val exactPmfHack = isExact.flatMap(_ => profile.flatMap(getExactPmfHack))
      exactPmfHack.map(pmf => profile.map(_.copy(pmf = pmf))).getOrElse(profile)
    }
  }

  private def getExactExtras: Map[String, String] = {
    if (dataType == null) {
      Map.empty
    } else {
      Map(TypeProfiler.IS_EXACT_EXTRAS_KEY -> Option(frequencySketch).exists(_.isExact).toString)
    }
  }

  private def getQuantileExtras: Map[String, String] = {
    val quantileExtras = Option(quantileSketch).flatMap { sketch =>
      val percentiles = Seq(5, 25, 50, 75, 95)
      val quantiles = percentiles.map(_ / 100.0)
      sketch.getQuantiles(quantiles).map { values =>
        val payload = quantiles.zip(values).map(qv => s""""${qv._1}": "${qv._2}"""").mkString("{", ",", "}")
        Map(TypeProfiler.QUANTILES_EXTRAS_KEY -> payload)
      }
    }
    quantileExtras.getOrElse(Map.empty)
  }

  private def getExactPmfHack(profile: DpColumn): Option[Seq[DpPmfBucket]] = {
    if (profile.items.size <= 1) {
      None
    } else {
      profile.data_type match {
        case null => None
        case "byte" | "short" | "int" | "long" if profile.enum_values.nonEmpty =>
          val normalizedValues = profile.enum_values.map(_.trim.toUpperCase)
          getDiscretePmfBuckets(profile.items, i => normalizedValues.indexOf(i.trim.toUpperCase))
        case "byte" | "short" | "int" | "long" => getDiscretePmfBuckets(profile.items, _.toLong.toDouble)
        case "float" | "double" => getDiscretePmfBuckets(profile.items, _.toDouble)
        case "boolean" if profile.enum_values.nonEmpty =>
          val normalizedFalseValue = profile.enum_values.headOption.map(_.trim.toUpperCase).orNull
          getDiscretePmfBuckets(profile.items, i => if (i.trim.toUpperCase != normalizedFalseValue) 1 else 0)
        case "boolean" => getDiscretePmfBuckets(profile.items, i => if (i.toBoolean) 1 else 0)
        case "date" if profile.format.nonEmpty =>
          val zoneId = ZoneId.of(profile.extras.getOrElse(TypeProfiler.ZONE_ID_EXTRAS_KEY, Dp.DEFAULT_ZONE_ID))
          val formatter = DateTimeFormatter.ofPattern(profile.format.orNull).withZone(zoneId)
          getDiscretePmfBuckets(
            profile.items, i => LocalDate.parse(i, formatter).atStartOfDay(zoneId).toEpochSecond.toDouble)
        case "timestamp" if profile.format.nonEmpty =>
          val zoneId = ZoneId.of(profile.extras.getOrElse(TypeProfiler.ZONE_ID_EXTRAS_KEY, Dp.DEFAULT_ZONE_ID))
          val formatter = DateTimeFormatter.ofPattern(profile.format.orNull).withZone(zoneId)
          getDiscretePmfBuckets(
            profile.items, i => LocalDateTime.parse(i, formatter).atZone(zoneId).toEpochSecond.toDouble)
        case _ => None
      }
    }
  }

  private def getDiscretePmfBuckets(items: Seq[DpItem], itemToDouble: String => Double): Option[Seq[DpPmfBucket]] = {
    val pmf = Try {
      val itemValues = items.map(i => itemToDouble(i.item)).sorted
      val deltas = itemValues.sliding(2).map(w => w.last - w.head).toSeq.sorted
      val medianDelta = deltas(deltas.size / 2)
      items.map(i => DpPmfBucket(i.item.toDouble - medianDelta / 2.0, i.item.toDouble + medianDelta / 2.0, i.count))
    }
    pmf.toOption
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
      case _: ArrayType => IterableProfiler.zero("array")
      case _: BinaryType => IterableProfiler.zero("binary")
      case _: MapType => IterableProfiler.zero("map")
      case _: StructType => StructProfiler.zero()
    }
    typeProfiler.asInstanceOf[TypeProfiler[Any]]
  }

  def zero(field: StructField, config: DpConfig): AnyProfiler = {
    val baseProfiler = AnyProfiler(
      name = field.name,
      dataType = null,
      nullable = field.nullable,
      count = 0L,
      countNulls = 0L,
      typeProfiler = getTypeProfiler(field.dataType, config),
      cardinalitySketch = null,
      frequencySketch = null,
      quantileSketch = null,
    )
    field.dataType match {
      case _: ArrayType | _: MapType | _: StructType => baseProfiler
      case _ =>
        baseProfiler.copy(
          dataType = field.dataType,
          cardinalitySketch = CardinalitySketch.create(config),
          frequencySketch = FrequencySketch.create(config),
          quantileSketch = QuantileSketch.create(config),
        )
    }
  }
}

private[dp] class StringProfiler(
    var countEmpty: Long,
    var min: String,
    var max: String,
    var maxLength: Int,
    val maxItemLength: Int,
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

  override def update(value: Any): Double = {
    value match {
      case s: String => updateString(s)
    }
    Double.NaN
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
            max_length = Some(maxLength),
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
      maxItemLength = config.max_item_length,
    )
  }
}

private[dp] class IntegralProfiler(
    var countZero: Long,
    var min: Long,
    var max: Long,
    var sum: Long,
    val zoneId: ZoneId,
) extends TypeProfiler[Long] {

  def updateLong(value: Long): Double = {
    if (value == 0L) {
      countZero += 1
    }
    min = math.min(min, value)
    max = math.max(max, value)
    sum += value
    value.toDouble
  }

  override def update(value: Any): Double = {
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
          val integralExtras = Map(TypeProfiler.MAX_LENGTH_IN_BITS_EXTRAS_KEY -> maxLengthInBits.toString)
          profile.copy(
            min = Some(itemToString(min)),
            max = Some(itemToString(max)),
            mean = Some(math.round(sum.toDouble / countNonNulls.toDouble).toDouble),
            count_zeros = Some(countZero),
            max_length = Some((maxLengthInBits + 7) / 8),
            extras = profile.extras ++ integralExtras,
          )
      }
    }
  }

  def toFractionalProfiler: FractionalProfiler = {
    new FractionalProfiler(
      countNaN = 0L,
      countZero = countZero,
      min = min.toDouble,
      max = max.toDouble,
      sum = sum.toDouble,
      requiresDoublePrecision = maxLengthInBits >= 24,
    )
  }

  private def maxLengthInBits: Int = {
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
      zoneId = ZoneId.of(config.time_zone_id.getOrElse(Dp.DEFAULT_ZONE_ID)),
    )
  }
}

private[dp] class FractionalProfiler(
    var countNaN: Long,
    var countZero: Long,
    var min: Double,
    var max: Double,
    var sum: Double,
    var requiresDoublePrecision: Boolean,
) extends TypeProfiler[Double] {

  def updateDouble(value: Double): Double = {
    if (value.isNaN) {
      countNaN += 1
    } else {
      if (value == 0.0) {
        countZero += 1
      }
      min = math.min(min, value)
      max = math.max(max, value)
      sum = sum + value
    }
    value
  }

  override def update(value: Any): Double = {
    value match {
      case f: Float => updateDouble(f)
      case d: Double => updateDouble(d)
      case d: Decimal => updateDouble(d.toDouble)
      case bd: java.math.BigDecimal => updateDouble(bd.doubleValue)
      case bi: java.math.BigInteger => updateDouble(bi.doubleValue)
      case bd: scala.math.BigDecimal => updateDouble(bd.doubleValue)
      case bi: scala.math.BigInt => updateDouble(bi.doubleValue)
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
          val maxLength = DataType.fromJson(profile.data_type_json) match {
            case FloatType | StringType if !requiresDoublePrecision => Some(4L)
            case FloatType | DoubleType | StringType => Some(8L)
            case dt: DecimalType if DecimalType.is32BitDecimalType(dt) => Some(4L)
            case dt: DecimalType if DecimalType.is64BitDecimalType(dt) => Some(8L)
            case dt: DecimalType => Some(math.ceil(dt.precision / math.log10(2.0) / 8).toLong)
          }
          profile.copy(
            min = Some(itemToString(min)),
            max = Some(itemToString(max)),
            mean = Some(sum / (countNonNulls - countNaN)),
            count_empty = Some(countNaN),
            count_zeros = Some(countZero),
            max_length = maxLength,
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
      requiresDoublePrecision = dataType == DoubleType,
    )
  }
}

private[dp] class IterableProfiler(
    typeName: String,
    var countEmpty: Long,
    var minLength: Int,
    var maxLength: Int,
) extends TypeProfiler[IterableOnce[_]] {

  def updateIterable(value: IterableOnce[_]): Unit = {
    if (value.isEmpty) {
      countEmpty += 1
    }
    val valueSize = value.size
    if (minLength > valueSize) {
      minLength = valueSize
    }
    if (maxLength < valueSize) {
      maxLength = valueSize
    }
  }

  override def update(value: Any): Double = {
    value match {
      case io: IterableOnce[_] => updateIterable(io)
    }
    Double.NaN
  }

  override def merge(that: TypeProfiler[IterableOnce[_]]): IterableProfiler = {
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
        case 0L => profile.copy(data_type = typeName)
        case _ =>
          profile.copy(
            data_type = typeName,
            count_empty = Some(countEmpty),
            max_length = Some(maxLength),
          )
      }
    }
  }
}

private[dp] object IterableProfiler {
  def zero(typeName: String): IterableProfiler = {
    new IterableProfiler(
      typeName = typeName,
      countEmpty = 0L,
      minLength = 0,
      maxLength = 0,
    )
  }
}

private[dp] class StructProfiler() extends TypeProfiler[AnyRef] {
  override def update(value: Any): Double = {
    Double.NaN
  }

  override def merge(that: TypeProfiler[AnyRef]): StructProfiler = {
    this
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = {
    baseProfile.map(_.copy(data_type = "struct"))
  }
}

private[dp] object StructProfiler {
  def zero(): StructProfiler = {
    new StructProfiler()
  }
}

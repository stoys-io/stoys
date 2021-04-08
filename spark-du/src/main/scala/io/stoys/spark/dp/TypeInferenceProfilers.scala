package io.stoys.spark.dp

import org.apache.spark.sql.types._

import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.ChronoField
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

private[dp] trait TypeInferenceProfiler {
  def update(stringValue: String): Boolean

  def merge(that: TypeInferenceProfiler): TypeInferenceProfiler

  def next(config: DpConfig): Seq[TypeInferenceProfiler] = Seq.empty

  def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn]
}

private[dp] case object NullTypeInferenceProfiler extends TypeInferenceProfiler {
  override def update(stringValue: String): Boolean = false

  override def merge(that: TypeInferenceProfiler): TypeInferenceProfiler = {
    that
  }

  override def next(config: DpConfig): Seq[TypeInferenceProfiler] = {
    val integralTIP = IntegralTypeInferenceProfiler(IntegralProfiler.zero(LongType, config))
    val fractionalTIP = FractionalTypeInferenceProfiler(FractionalProfiler.zero(DoubleType, config))
    // TODO: add DecimalTypeProfiler?
    val zoneId = ZoneId.of(config.time_zone_id.getOrElse(Dp.DEFAULT_TIME_ZONE_ID))
    val dateTIPs = config.type_inference_config.date_formats
        .map(f => DateTimeTypeInferenceProfiler(IntegralProfiler.zero(LongType, config), DateType, f, zoneId))
    val timestampTIPs = config.type_inference_config.timestamp_formats
        .map(f => DateTimeTypeInferenceProfiler(IntegralProfiler.zero(LongType, config), TimestampType, f, zoneId))
    Seq(integralTIP, fractionalTIP) ++ dateTIPs ++ timestampTIPs
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = None
}

private[dp] case class IntegralTypeInferenceProfiler(
    integralProfiler: IntegralProfiler
) extends TypeInferenceProfiler {

  override def update(stringValue: String): Boolean = {
    try {
      integralProfiler.update(stringValue.toLong)
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  override def merge(that: TypeInferenceProfiler): TypeInferenceProfiler = {
    that match {
      case that: IntegralTypeInferenceProfiler => IntegralTypeInferenceProfiler(
        this.integralProfiler.merge(that.integralProfiler)
      )
      case that: FractionalTypeInferenceProfiler => this.toFractionalTypeInferenceProfiler.merge(that)
      case _ => null
    }
  }

  override def next(config: DpConfig): Seq[TypeInferenceProfiler] = {
    Seq(toFractionalTypeInferenceProfiler)
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = {
    integralProfiler.profile(baseProfile, config).map { profile =>
      if (config.type_inference_config.prefer_boolean_for_01
          && integralProfiler.min >= 0 && integralProfiler.max <= 1) {
        profile.copy(data_type = BooleanType.typeName, enum_values = Seq("0", "1"))
      } else if (config.type_inference_config.prefer_integer
          && integralProfiler.min >= Int.MinValue && integralProfiler.max <= Int.MaxValue) {
        profile.copy(data_type = IntegerType.typeName)
      } else {
        profile.copy(data_type = LongType.typeName)
      }
    }
  }

  def toFractionalTypeInferenceProfiler: FractionalTypeInferenceProfiler = {
    FractionalTypeInferenceProfiler(integralProfiler.toFractionalProfiler)
  }
}

private[dp] case class FractionalTypeInferenceProfiler(
    fractionalProfiler: FractionalProfiler
) extends TypeInferenceProfiler {

  override def update(stringValue: String): Boolean = {
    // TODO: count digits to infer float or double
    try {
      fractionalProfiler.update(stringValue.toDouble)
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  override def merge(that: TypeInferenceProfiler): TypeInferenceProfiler = {
    that match {
      case that: IntegralTypeInferenceProfiler => this.merge(that.toFractionalTypeInferenceProfiler)
      case that: FractionalTypeInferenceProfiler => FractionalTypeInferenceProfiler(
        fractionalProfiler = this.fractionalProfiler.merge(that.fractionalProfiler)
      )
      case _ => null
    }
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = {
    val fp = fractionalProfiler
    if (config.type_inference_config.prefer_float && fp.min >= Float.MinValue && fp.max <= Float.MaxValue) {
      fp.profile(baseProfile, _.toFloat.toString).map(_.copy(data_type = FloatType.typeName, max_length = Some(4)))
    } else {
      fp.profile(baseProfile, _.toString).map(_.copy(data_type = DoubleType.typeName, max_length = Some(8)))
    }
  }
}

private[dp] case class DateTimeTypeInferenceProfiler(
    integralProfiler: IntegralProfiler,
    dataType: DataType,
    format: String,
    zoneId: ZoneId
) extends TypeInferenceProfiler {

  private val formatter = DateTimeFormatter.ofPattern(format).withZone(zoneId)

  override def update(stringValue: String): Boolean = {
    try {
      val zonedDateTime = dataType match {
        case DateType => LocalDate.parse(stringValue, formatter).atStartOfDay(zoneId)
        case TimestampType => LocalDateTime.parse(stringValue, formatter).atZone(zoneId)
      }
      integralProfiler.update(zonedDateTime.toEpochSecond)
      true
    } catch {
      case _: DateTimeParseException => false
    }
  }

  override def merge(that: TypeInferenceProfiler): TypeInferenceProfiler = {
    that match {
      case that: DateTimeTypeInferenceProfiler if this.dataType == that.dataType && this.format == that.format =>
        DateTimeTypeInferenceProfiler(this.integralProfiler.merge(that.integralProfiler), dataType, format, zoneId)
      case _ => null
    }
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = {
    integralProfiler.profile(baseProfile, v => formatter.format(Instant.ofEpochSecond(v))).map(p => p.copy(
      data_type = dataType.typeName,
      format = Option(format),
      min = p.min.map(reformatItem),
      max = p.max.map(reformatItem),
      items = p.items.map(i => i.copy(item = reformatItem(i.item))) // TODO: Should we just keep the original strings?
    ))
  }

  private def reformatItem(item: String): String = {
    item match {
      case item if item.isEmpty => item
      case item =>
        val epochSeconds = formatter.parse(item) match {
          case ta if ta.isSupported(ChronoField.INSTANT_SECONDS) => Instant.from(ta).toEpochMilli / 1000L
          case ta if ta.isSupported(ChronoField.SECOND_OF_DAY) => LocalDateTime.from(ta).atZone(zoneId).toEpochSecond
          case ta if ta.isSupported(ChronoField.EPOCH_DAY) => LocalDate.from(ta).atStartOfDay(zoneId).toEpochSecond
        }
        epochSeconds.toString
    }
  }
}

private[dp] class TypeInferenceStringProfiler(
    var stringProfiler: StringProfiler,
    var typeInferenceProfiler: TypeInferenceProfiler,
    val config: DpConfig
) extends TypeProfiler[String] {

  def updateString(stringValue: String): Unit = {
    stringProfiler.update(stringValue)
    if (stringValue.nonEmpty && typeInferenceProfiler != null) {
      if (!typeInferenceProfiler.update(stringValue)) {
        val next = typeInferenceProfiler.next(config).find(_.update(stringValue))
        typeInferenceProfiler = next.orNull
      }
    }
  }

  override def update(value: Any): Unit = {
    value match {
      case s: String => updateString(s)
    }
  }

  override def merge(that: TypeProfiler[String]): TypeInferenceStringProfiler = {
    that match {
      case that: TypeInferenceStringProfiler =>
        val stringProfiler = this.stringProfiler.merge(that.stringProfiler)
        val typeInferenceProfilers = Seq(Option(this.typeInferenceProfiler), Option(that.typeInferenceProfiler)).flatten
        val typeInferenceProfiler = typeInferenceProfilers.reduceOption((a, b) => a.merge(b)).orNull
        new TypeInferenceStringProfiler(stringProfiler, typeInferenceProfiler, this.config)
    }
  }

  override def profile(baseProfile: Option[DpColumn], config: DpConfig): Option[DpColumn] = {
    val stringProfile = stringProfiler.profile(baseProfile, config)
    val typeEnumProfile = Option(typeInferenceProfiler).flatMap(_.profile(baseProfile, config))
        .orElse(stringProfile.flatMap(p => TypeInferenceStringProfiler.matchEnumValues(p, config)))
        .map(p => p.copy(extras = p.extras + ("is_inferred_type" -> "true")))
    val profile = (typeEnumProfile, stringProfile) match {
      case (None, None) => None
      case (Some(typeEnumProfile), None) => Some(typeEnumProfile)
      case (None, Some(stringProfile)) => Some(stringProfile)
      case (Some(typeEnumProfile), Some(stringProfile)) =>
        Some(
          typeEnumProfile.copy(
            count_empty = Seq(typeEnumProfile.count_empty, stringProfile.count_empty).flatten.reduceOption(_ + _),
            count_unique = typeEnumProfile.count_unique.orElse(stringProfile.count_unique),
            max_length = typeEnumProfile.max_length.orElse(stringProfile.max_length),
            items = if (typeEnumProfile.items.nonEmpty) typeEnumProfile.items else stringProfile.items
          )
        )
    }
    profile.map { profile =>
      val preferNullable = !config.type_inference_config.prefer_not_nullable
      val nullable = profile.count_nulls.getOrElse(0L) > 0L || (preferNullable && profile.nullable)
      profile.copy(nullable = nullable)
    }
  }
}

private[dp] object TypeInferenceStringProfiler {
  def zero(config: DpConfig): TypeInferenceStringProfiler = {
    new TypeInferenceStringProfiler(
      stringProfiler = StringProfiler.zero(config),
      typeInferenceProfiler = NullTypeInferenceProfiler,
      config
    )
  }

  private case class EnumInfo(item: DpItem, index: Int)

  def matchEnumValues(profile: DpColumn, config: DpConfig): Option[DpColumn] = {
    val items = profile.items
    if (profile.data_type != StringType.typeName || items.isEmpty) {
      None
    } else {
      val normalizedGroupedItems = items.groupBy(_.item.trim.toUpperCase).filterKeys(_.nonEmpty)
      val normalizedItemCounts = normalizedGroupedItems.map(kv => DpItem(kv._1, kv._2.map(_.count).sum))
      val formatsFound = config.type_inference_config.enum_values.flatMap { enumValues =>
        val valuesToIndex = enumValues.values.zipWithIndex.map(vi => vi._1.trim.toUpperCase -> vi._2).toMap
        val enumInfo = normalizedItemCounts.map(i => EnumInfo(i, valuesToIndex.getOrElse(i.item, -1))).toSeq
        val enumIndexes = enumInfo.map(_.index)
        if (enumIndexes.nonEmpty && enumIndexes.min >= 0) {
          val dataType = enumValues.name match {
            case DpTypeInferenceConfig.EnumValues.BOOLEAN_NAME => BooleanType
            case null | "" => StringType
          }
          val count = enumInfo.map(_.item.count).sum
          val sum = enumInfo.map(i => i.index * i.item.count).sum
          val countZeros = enumInfo.find(_.index == 0).map(_.item.count).getOrElse(0L)
          Some(
            profile.copy(
              data_type = dataType.typeName,
              enum_values = enumValues.values,
              count_empty = None,
              count_unique = Some(enumInfo.size),
              count_zeros = Some(countZeros),
              max_length = Option(normalizedItemCounts.map(_.item.length).max),
              min = Option(enumValues.values(enumIndexes.min)),
              max = Option(enumValues.values(enumIndexes.max)),
              mean = Option(sum.toDouble / count),
              items = enumInfo.sortBy(_.index).map(_.item)
            )
          )
        } else {
          None
        }
      }
      formatsFound.headOption
    }
  }
}

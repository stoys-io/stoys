package io.stoys.spark.dp

import io.stoys.spark.SToysException
import io.stoys.spark.dp.sketches.functions.{items_sketch, kll_floats_sketch}
import io.stoys.spark.dp.sketches.{ItemsSketchAggregator, KllFloatsSketchAggregator}
import org.apache.spark.sql.catalyst.expressions.FormatNumber
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset}

import java.time.Instant

private[dp] object DpFramework {
  type ColumnProfilers = Map[String, Column]

  private def createMetadataColumnProfilers(fieldPath: FieldPath): ColumnProfilers = {
    Map(
      "name" -> lit(fieldPath.toString),
      "data_type" -> lit(fieldPath.dataType.typeName)
    )
  }

  private def createDefaultColumnProfilers(fieldPath: FieldPath): ColumnProfilers = {
    val column = fieldPath.toColumn
    columnProfileEnumKeyToStringKey(Map(
      DpProfilerName.COUNT -> count(lit(1)),
      DpProfilerName.COUNT_EMPTY -> lit(null).cast(LongType),
      DpProfilerName.COUNT_NULLS -> count_if(isnull(column)),
      DpProfilerName.COUNT_UNIQUE -> approx_count_distinct(column),
      DpProfilerName.COUNT_ZEROS -> lit(null).cast(LongType),
      DpProfilerName.MAX_LENGTH -> max(length(column)),
      DpProfilerName.MIN -> min(column).cast(StringType),
      DpProfilerName.MAX -> max(column).cast(StringType),
      DpProfilerName.MEAN -> mean(column).cast(StringType),
      DpProfilerName.HISTOGRAM -> lit(null).cast(KllFloatsSketchAggregator.dataType),
      DpProfilerName.ITEMS -> lit(null).cast(ItemsSketchAggregator.dataType)
    ))
  }

  private def createTypeBasedColumnProfilers(fieldPath: FieldPath, config: DpConfig): ColumnProfilers = {
    val column = fieldPath.toColumn
    val typeBasedColumnProfilers = fieldPath.dataType match {
      case ByteType | ShortType | IntegerType | LongType =>
        Map(
          DpProfilerName.COUNT_ZEROS -> count_if(column === lit(0)),
          DpProfilerName.MIN -> format_float(min(column)),
          DpProfilerName.MAX -> format_float(max(column)),
          DpProfilerName.MEAN -> format_float(mean(column)),
          DpProfilerName.HISTOGRAM -> kll_floats_sketch(column.cast(FloatType), config.buckets),
          DpProfilerName.ITEMS -> items_sketch(column, config.items)
        )
      case FloatType | DoubleType | _: DecimalType =>
        val columnWithNullInsteadOfNan = nanvl(column, lit(null))
        Map(
          DpProfilerName.COUNT_EMPTY -> count_if(isnan(column)),
          DpProfilerName.COUNT_ZEROS -> count_if(column === lit(0.0)),
          DpProfilerName.MIN -> format_float(min(columnWithNullInsteadOfNan)),
          DpProfilerName.MAX -> format_float(max(columnWithNullInsteadOfNan)),
          DpProfilerName.MEAN -> format_float(mean(columnWithNullInsteadOfNan)),
          DpProfilerName.HISTOGRAM -> kll_floats_sketch(column.cast(FloatType), config.buckets),
          DpProfilerName.ITEMS -> items_sketch(column, config.items)
        )
      case StringType | BinaryType =>
        val columnTruncated = substring(column, 0, 1024)
        Map(
          DpProfilerName.COUNT_EMPTY -> count_if(length(column) === lit(0)),
          DpProfilerName.MEAN -> lit(null).cast(StringType),
          DpProfilerName.ITEMS -> items_sketch(columnTruncated, config.items)
        )
      case BooleanType =>
        Map(
          DpProfilerName.COUNT_ZEROS -> count_if(column === lit(false)),
          DpProfilerName.MEAN -> format_float(mean(column.cast(DoubleType))),
          DpProfilerName.ITEMS -> items_sketch(column, config.items)
        )
      case TimestampType | DateType =>
        val zeroTimestamp = Instant.parse("0001-01-01T00:00:00.000000Z").getEpochSecond
        Map(
          DpProfilerName.COUNT_ZEROS -> count_if(unix_timestamp(column) === lit(zeroTimestamp)),
          DpProfilerName.MEAN -> from_unixtime(mean(unix_timestamp(column))),
          DpProfilerName.HISTOGRAM ->
              kll_floats_sketch(unix_timestamp(column).cast(FloatType), config.buckets),
          DpProfilerName.ITEMS -> items_sketch(column, config.items)
        )
      case _: ArrayType | _: MapType =>
        val sizeOrNull = when(size(column) < lit(0), lit(null)).otherwise(size(column))
        Map(
          DpProfilerName.COUNT_EMPTY -> count_if(size(column) === lit(0)),
          DpProfilerName.MAX_LENGTH -> lit(null).cast(LongType),
          DpProfilerName.MIN -> format_float(min(sizeOrNull)),
          DpProfilerName.MAX -> format_float(max(sizeOrNull)),
          DpProfilerName.MEAN -> format_float(mean(sizeOrNull))
        )
      case _: StructType =>
        Map(
          DpProfilerName.MAX_LENGTH -> lit(null).cast(LongType),
          DpProfilerName.MIN -> lit(null).cast(StringType),
          DpProfilerName.MAX -> lit(null).cast(StringType),
          DpProfilerName.MEAN -> lit(null).cast(StringType)
        )
      case dt => throw new SToysException(s"Unknown data type $dt")
    }
    columnProfileEnumKeyToStringKey(typeBasedColumnProfilers)
  }

  private def columnProfileEnumKeyToStringKey(columnProfiler: Map[DpProfilerName, Column]): ColumnProfilers = {
    columnProfiler.map(kv => (kv._1.toString.toLowerCase(), kv._2))
  }

  private def columnProfileColumnsToDpColumnStruct(columnProfiler: ColumnProfilers): Column = {
    struct(columnProfiler.map(kv => kv._2.as(kv._1)).toArray: _*)
  }

  private def getColumnProfilers(fieldPath: FieldPath, config: DpConfig): Seq[ColumnProfilers] = {
    val childrenColumnProfilers = fieldPath.children.flatMap(child => getColumnProfilers(child, config))
    if (fieldPath.isRoot) {
      childrenColumnProfilers
    } else {
      val metadataProfilers = createMetadataColumnProfilers(fieldPath)
      val defaultProfilers = createDefaultColumnProfilers(fieldPath)
      val typeBasedProfilers = createTypeBasedColumnProfilers(fieldPath, config)
      val selfColumnProfilers = metadataProfilers ++ defaultProfilers ++ typeBasedProfilers
      selfColumnProfilers +: childrenColumnProfilers
    }
  }

  def computeDpResult[T](ds: Dataset[T], config: DpConfig): Dataset[DpResult] = {
    import ds.sparkSession.implicits._
    val columnProfilers = getColumnProfilers(FieldPath.fromRoot(ds.schema), config)
    val dpResultDs = ds.select(
      struct(count(lit(1)).as("rows")).as("table"),
      array(columnProfilers.map(columnProfileColumnsToDpColumnStruct): _*).as("columns")
    )
    dpResultDs.as[DpResult]
  }

  private def count_if(condition: Column): Column = {
    count(when(condition, lit(1)).otherwise(lit(null)))
    // Spark 3.1+:
//    new Column(CountIf(condition.expr).toAggregateExpression())
  }

  private def format_number_str(column: Column, fmt: String): Column = {
    new Column(FormatNumber(column.expr, lit(fmt).expr))
  }

  private def format_float(column: Column): Column = {
//    format_number_str(column, "##0.000 E0")
    format_number(column, 2)
  }

  object FieldPath {
    def fromRoot(structType: StructType): FieldPath = {
      FieldPath(structType, None, Seq.empty)
    }
  }

  case class FieldPath(dataType: DataType, private val column: Option[Column], private val path: Seq[String]) {
    def isRoot: Boolean = {
      column.isEmpty
    }

    def children: Seq[FieldPath] = {
      dataType match {
        // TODO: One cannot use explode inside a function. What else can we do??
//        case a: ArrayType =>
//          Seq(FieldPath(a.elementType, column.map(explode), path.dropRight(1) :+ path.last + "[]"))
//        case m: MapType =>
//          Seq(
//            FieldPath(m.keyType, column.map(c => explode(map_keys(c))), path.dropRight(1) :+ path.last + "{K}"),
//            FieldPath(m.valueType, column.map(c => explode(map_values(c))), path.dropRight(1) :+ path.last + "{V}")
//          )
        case s: StructType =>
          s.fields.toSeq.map { f =>
            FieldPath(f.dataType, column.map(_.getField(f.name)).orElse(Some(col(f.name))), path :+ f.name)
          }
        case _ => Seq.empty
      }
    }

    def toColumn: Column = {
      column.getOrElse(struct(col("*")).as("__root__"))
    }

    override def toString: String = {
      path.mkString(".")
    }
  }
}

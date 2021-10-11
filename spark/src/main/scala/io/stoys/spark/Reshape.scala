package io.stoys.spark

import io.stoys.scala.Strings
import io.stoys.spark.MetadataKeys.{ENUM_VALUES_KEY, FORMAT_KEY}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, Attribute, Cast, CreateArray, CreateMap, Expression, LambdaFunction, Literal, NamedLambdaVariable, TransformKeys, TransformValues}
import org.apache.spark.sql.catalyst.util.usePrettyExpression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

object Reshape {
  /**
   * [[reshape]][T] is similar to spark's own [[Dataset.as]][T] but way more powerful and configurable.
   *
   * See [[ReshapeConfig]] for supported configuration and/or unit tests for examples.
   *
   * BEWARE: The same config is applied on all the columns (even nested).
   *
   * @param ds input [[Dataset]] or [[DataFrame]]
   * @param config reshape configuration - see [[ReshapeConfig]] for details
   * @tparam T case class to which we are casting the df
   * @return [[Dataset]][T] of the desired shape
   * @throws ReshapeException if the input [[Dataset]] cannot be reshaped to the desired shape
   */
  @throws(classOf[ReshapeException])
  def reshape[T <: Product : TypeTag](ds: Dataset[_], config: ReshapeConfig = ReshapeConfig.default): Dataset[T] = {
    import ds.sparkSession.implicits._
    val targetSchema = ScalaReflection.schemaFor[T].dataType
    reshapeToDF(ds, targetSchema, config).as[T]
  }

  @throws(classOf[ReshapeException])
  def reshapeToDF(ds: Dataset[_], targetSchema: DataType, config: ReshapeConfig = ReshapeConfig.default): DataFrame = {
    val sourceSchema = ds.schema
    targetSchema match {
      case targetSchema: StructType if sourceSchema == targetSchema => ds.toDF()
      case targetSchema: StructType =>
        val attributes = ds.queryExecution.analyzed.output
        reshapeStructType(sourceSchema, targetSchema, config, sourceColumn = null, attributes) match {
          case Left(errors) => throw new ReshapeException(errors)
          case Right(columns) => ds.select(columns: _*)
        }
      case dt => throw new SToysException(s"Unsupported target schema '$dt'. Only StructType is supported.")
    }
  }

  private def defaultValueExpression(dataType: DataType): Expression = {
    dataType match {
      case ArrayType(internalType: DataType, _) =>
        CreateArray(Seq(Literal.default(internalType)))
      case MapType(keyDataType: DataType, valueDataType: DataType, _) =>
        CreateMap(Seq(Literal.default(keyDataType), Literal.default(valueDataType)))
      case _ => Literal.default(dataType)
    }
  }

  private def nullValueExpression(dataType: DataType): Expression = {
    dataType match {
      case ArrayType(internalType: DataType, _) =>
        CreateArray(Seq(Literal.create(null, internalType)))
      case MapType(keyDataType: DataType, valueDataType: DataType, _) =>
        CreateMap(Seq(Literal.create(null, keyDataType), Literal.create(null, valueDataType)))
      case _ => Literal.create(null, dataType)
    }
  }

  private def getNestedColumn(sourceColumn: Column, fieldName: String): Column = {
    (sourceColumn, fieldName) match {
      case (null, null) => null
      case (sourceColumn, null) => sourceColumn
      case (null, fieldName) => col(fieldName)
      case (sourceColumn, fieldName) => sourceColumn.getField(fieldName)
    }
  }

  private def getNormalizeFieldName(field: StructField, fieldMatchingStrategy: ReshapeFieldMatchingStrategy): String = {
    fieldMatchingStrategy match {
      case ReshapeFieldMatchingStrategy.UNDEFINED | ReshapeFieldMatchingStrategy.NAME_DEFAULT | null =>
        field.name.trim.toLowerCase
      case ReshapeFieldMatchingStrategy.NAME_EXACT | ReshapeFieldMatchingStrategy.INDEX => field.name
      case ReshapeFieldMatchingStrategy.NAME_NORMALIZED => Strings.toSnakeCase(field.name.trim)
    }
  }

  private[stoys] def reshapeDataType(sourceDataType: DataType, targetDataType: DataType, config: ReshapeConfig,
      sourceExpression: Expression, normalizedFieldPath: String): Either[List[ReshapeError], Column] = {
    val reshapedField = reshapeStructField(StructField(null, sourceDataType), StructField(null, targetDataType),
      config, new Column(sourceExpression), sourceAttribute = None, normalizedFieldPath)
    reshapedField match {
      case Left(errors) => Left(errors)
      case Right(column) => Right(column.head)
    }
  }

  private def reshapeStructField(sourceField: StructField, targetField: StructField, config: ReshapeConfig,
      sourceColumn: Column, sourceAttribute: Option[Attribute],
      normalizedFieldPath: String): Either[List[ReshapeError], List[Column]] = {
    val errors = mutable.Buffer.empty[ReshapeError]
    var column = sourceAttribute.map(a => new Column(a)).getOrElse(getNestedColumn(sourceColumn, sourceField.name))
    if (sourceField.nullable && !targetField.nullable) {
      if (config.fail_on_ignoring_nullability) {
        errors += ReshapeError(normalizedFieldPath, "is nullable but target column is not")
      } else if (config.fill_default_values) {
        column = coalesce(column, new Column(defaultValueExpression(targetField.dataType)))
      }
    }
    (sourceField.dataType, targetField.dataType) match {
      case (sourceDT, targetDT) if sourceDT == targetDT => // same shape

      case (sourceST: StructType, targetST: StructType) =>
        reshapeStructType(sourceST, targetST, config, column, sourceAttributes = Seq.empty) match {
          case Left(nestedErrors) => errors ++= nestedErrors
          case Right(nestedColumns) => column = struct(nestedColumns: _*)
        }

      case (sourceAT: ArrayType, targetAT: ArrayType) =>
        val elementFieldPath = s"$normalizedFieldPath[]"
        val identifier = Strings.toWordCharactersCollapsing(elementFieldPath)
        val lambdaVariable = NamedLambdaVariable(identifier, sourceAT.elementType, sourceAT.containsNull)
        reshapeDataType(sourceAT.elementType, targetAT.elementType, config, lambdaVariable, elementFieldPath) match {
          case Left(nestedErrors) =>
            errors ++= nestedErrors
          case Right(nestedColumn) =>
            val lambdaFunction = LambdaFunction(nestedColumn.expr, Seq(lambdaVariable))
            column = new Column(ArrayTransform(column.expr, lambdaFunction))
        }

      case (sourceMT: MapType, targetMT: MapType) =>
        val keyFieldPath = s"$normalizedFieldPath{K}"
        val valueFieldPath = s"$normalizedFieldPath{V}"
        val keyIdentifier = Strings.toWordCharactersCollapsing(keyFieldPath)
        val valueIdentifier = Strings.toWordCharactersCollapsing(valueFieldPath)
        val keyLambdaVariable = NamedLambdaVariable(keyIdentifier, sourceMT.keyType, nullable = false)
        val valueLambdaVariable = NamedLambdaVariable(valueIdentifier, sourceMT.valueType, sourceMT.valueContainsNull)
        if (sourceMT.keyType != targetMT.keyType) {
          reshapeDataType(sourceMT.keyType, targetMT.keyType, config, keyLambdaVariable, keyFieldPath) match {
            case Left(nestedErrors) =>
              errors ++= nestedErrors
            case Right(nestedColumn) =>
              val lambdaFunction = LambdaFunction(nestedColumn.expr, Seq(keyLambdaVariable, valueLambdaVariable))
              column = new Column(TransformKeys(column.expr, lambdaFunction))
          }
        }
        if (sourceMT.valueType != targetMT.valueType) {
          reshapeDataType(sourceMT.valueType, targetMT.valueType, config, valueLambdaVariable, valueFieldPath) match {
            case Left(nestedErrors) =>
              errors ++= nestedErrors
            case Right(nestedColumn) =>
              val lambdaFunction = LambdaFunction(nestedColumn.expr, Seq(keyLambdaVariable, valueLambdaVariable))
              column = new Column(TransformValues(column.expr, lambdaFunction))
          }
        }

      case (_: StringType, _: IntegerType | LongType) if targetField.metadata.contains(ENUM_VALUES_KEY) =>
        val enumValues = targetField.metadata.getStringArray(ENUM_VALUES_KEY).map(_.toUpperCase)
        column = upper(trim(column))
        column = array_position(lit(enumValues), column).cast(IntegerType) - 1
      case (_: IntegerType | LongType, _: StringType) if sourceField.metadata.contains(ENUM_VALUES_KEY) =>
        val enumValues = sourceField.metadata.getStringArray(ENUM_VALUES_KEY)
        column = element_at(lit(enumValues), column + 1)
      case (_: StringType, _: BooleanType) if targetField.metadata.contains(ENUM_VALUES_KEY) =>
        val Array(falseValue, trueValue) = targetField.metadata.getStringArray(ENUM_VALUES_KEY).map(_.toUpperCase)
        column = upper(trim(column))
        column = when(column === falseValue, false).when(column === trueValue, true).otherwise(null)
      case (_: BooleanType, _: StringType) if sourceField.metadata.contains(ENUM_VALUES_KEY) =>
        val Array(falseValue, trueValue) = sourceField.metadata.getStringArray(ENUM_VALUES_KEY).map(_.toUpperCase)
        column = when(column, lit(trueValue)).otherwise(lit(falseValue))
      case (_: StringType, _: DateType) if targetField.metadata.contains(FORMAT_KEY) =>
        column = to_date(column, targetField.metadata.getString(FORMAT_KEY))
      case (_: StringType, _: TimestampType) if targetField.metadata.contains(FORMAT_KEY) =>
        column = to_timestamp(column, targetField.metadata.getString(FORMAT_KEY))
      case (_: DateType | TimestampType, _: StringType) if sourceField.metadata.contains(FORMAT_KEY) =>
        column = date_format(column, sourceField.metadata.getString(FORMAT_KEY))

      case (_: StringType, _: DateType) if config.date_format.isDefined =>
        column = to_date(column, config.date_format.orNull)
      case (_: StringType, _: TimestampType) if config.timestamp_format.isDefined =>
        column = to_timestamp(column, config.timestamp_format.orNull)
      case (sourceDataType, targetDataType) if config.coerce_types && Cast.canCast(sourceDataType, targetDataType) =>
        column = column.cast(targetDataType)
      case (sourceDataType, targetDataType) =>
        errors += ReshapeError(normalizedFieldPath, s"of type $sourceDataType cannot be casted to $targetDataType")
    }

    if (errors.isEmpty) {
      Right(List(Option(targetField.name).map(column.as).getOrElse(column)))
    } else {
      Left(errors.toList)
    }
  }

  private def reshapeStructType(sourceStruct: StructType, targetStruct: StructType, config: ReshapeConfig,
      sourceColumn: Column, sourceAttributes: Seq[Attribute]): Either[List[ReshapeError], List[Column]] = {
    val fieldMapping = config.field_matching_strategy match {
      case ReshapeFieldMatchingStrategy.UNDEFINED | ReshapeFieldMatchingStrategy.NAME_DEFAULT
          | ReshapeFieldMatchingStrategy.NAME_EXACT | ReshapeFieldMatchingStrategy.NAME_NORMALIZED | null =>
        getNameBasedFieldMapping(sourceStruct, targetStruct, config)
      case ReshapeFieldMatchingStrategy.INDEX =>
        getIndexBasedFieldMapping(sourceStruct, targetStruct, config)
    }

    val sourceColumnPath = Option(sourceColumn).map(c => usePrettyExpression(c.expr).sql)

    val resultColumns = fieldMapping.map { fm =>
      val normalizedFieldPath = sourceColumnPath.map(cp => s"$cp.${fm.normalizedName}").getOrElse(fm.normalizedName)
      (fm.sourceFields, fm.targetFields) match {
        case (Nil, target :: Nil) =>
          if (target.nullable && config.fill_missing_nulls) {
            Right(List(new Column(nullValueExpression(target.dataType)).as(target.name)))
          } else if (config.fill_default_values) {
            Right(List(new Column(defaultValueExpression(target.dataType)).as(target.name)))
          } else {
            Left(List(ReshapeError(normalizedFieldPath, "is missing")))
          }
        case (source :: Nil, target :: Nil) =>
          reshapeStructField(source, target, config, sourceColumn, sourceAttribute = None, normalizedFieldPath)
        case (sources, target :: Nil) =>
          config.conflict_resolution match {
            case ReshapeConflictResolution.ERROR | ReshapeConflictResolution.UNDEFINED | null =>
              Left(List(ReshapeError(normalizedFieldPath, s"has ${sources.size} conflicting occurrences")))
            case ReshapeConflictResolution.FIRST =>
              val firstAttribute = sourceAttributes.find(_.name == target.name)
              reshapeStructField(sources.head, target, config, sourceColumn, firstAttribute, normalizedFieldPath)
            case ReshapeConflictResolution.LAST =>
              val lastAttribute = sourceAttributes.filter(_.name == target.name).lastOption
              reshapeStructField(sources.last, target, config, sourceColumn, lastAttribute, normalizedFieldPath)
          }
        case (sources, Nil) =>
          if (config.fail_on_extra_column) {
            Left(List(ReshapeError(normalizedFieldPath, s"unexpectedly present (${sources.size}x)")))
          } else {
            if (config.drop_extra_columns) {
              Right(List.empty)
            } else {
              val attributes = sourceAttributes.filter(_.name == fm.normalizedName)
              if (attributes.nonEmpty) {
                Right(attributes.map(a => new Column(a)))
              } else {
                Right(sources.map(f => getNestedColumn(sourceColumn, f.name)))
              }
            }
          }
        case (_, targets) if targets.size > 1 =>
          Left(List(ReshapeError(normalizedFieldPath, s"has ${targets.size} occurrences in target struct")))
      }
    }

    resultColumns.toList.partition(_.isLeft) match {
      case (Nil, columns) => Right(columns.flatMap(_.right.toOption).flatten)
      case (errors, _) => Left(errors.flatMap(_.left.toOption).flatten)
    }
  }

  private case class FieldMapping(
      normalizedName: String,
      sourceFields: List[StructField],
      targetFields: List[StructField]
  )

  private def getIndexBasedFieldMapping(sourceStruct: StructType, targetStruct: StructType,
      config: ReshapeConfig): Seq[FieldMapping] = {
    def getNormalizedName(sf: StructField) = getNormalizeFieldName(sf, ReshapeFieldMatchingStrategy.NAME_EXACT)

    val sourceNormalizedFieldNamesWithIndices = sourceStruct.fields.map(getNormalizedName).zipWithIndex
    val targetNormalizedFieldNamesWithIndices = targetStruct.fields.map(getNormalizedName).zipWithIndex
    val orderedTargetNormalizedFieldNamesWithIndices = config.sort_order match {
      case ReshapeSortOrder.ALPHABETICAL =>
        targetNormalizedFieldNamesWithIndices.sortBy(_._1)
      case ReshapeSortOrder.SOURCE | ReshapeSortOrder.TARGET | ReshapeSortOrder.UNDEFINED | null =>
        targetNormalizedFieldNamesWithIndices
    }
    val extraSourceNormalizedFieldNamesWithIndices =
      sourceNormalizedFieldNamesWithIndices.drop(targetNormalizedFieldNamesWithIndices.length)
    (orderedTargetNormalizedFieldNamesWithIndices ++ extraSourceNormalizedFieldNamesWithIndices).map {
      case (normalizedFieldName, i) =>
        val sourceField = if (sourceStruct.isDefinedAt(i)) Some(sourceStruct.fields(i)) else None
        val targetField = if (targetStruct.isDefinedAt(i)) Some(targetStruct.fields(i)) else None
        FieldMapping(normalizedFieldName, sourceField.toList, targetField.toList)
    }
  }

  private def getNameBasedFieldMapping(sourceStruct: StructType, targetStruct: StructType,
      config: ReshapeConfig): Seq[FieldMapping] = {
    def getNormalizedName(sf: StructField) = getNormalizeFieldName(sf, config.field_matching_strategy)

    val sourceFieldsByName = sourceStruct.fields.toList.groupBy(getNormalizedName)
    val targetFieldsByName = targetStruct.fields.toList.groupBy(getNormalizedName)
    val normalizedFieldNames = config.sort_order match {
      case ReshapeSortOrder.ALPHABETICAL =>
        (sourceStruct.fields ++ targetStruct.fields).map(getNormalizedName).distinct.sorted
      case ReshapeSortOrder.SOURCE | ReshapeSortOrder.UNDEFINED | null =>
        (sourceStruct.fields ++ targetStruct.fields).map(getNormalizedName).distinct
      case ReshapeSortOrder.TARGET =>
        (targetStruct.fields ++ sourceStruct.fields).map(getNormalizedName).distinct
    }
    normalizedFieldNames.map { normalizedFieldName =>
      val sourceFields = sourceFieldsByName.getOrElse(normalizedFieldName, List.empty)
      val targetFields = targetFieldsByName.getOrElse(normalizedFieldName, List.empty)
      FieldMapping(normalizedFieldName, sourceFields, targetFields)
    }
  }
}

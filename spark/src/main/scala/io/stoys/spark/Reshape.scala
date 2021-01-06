package io.stoys.spark

import io.stoys.scala.Strings
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, Cast, CreateArray, CreateMap, Expression, LambdaFunction, Literal, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.util.usePrettyExpression
import org.apache.spark.sql.functions.{coalesce, col, struct}
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
    val targetSchema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    reshapeToDF(ds, targetSchema, config).as[T]
  }

  @throws(classOf[ReshapeException])
  def reshapeToDF(ds: Dataset[_], targetSchema: DataType, config: ReshapeConfig = ReshapeConfig.default): DataFrame = {
    val sourceSchema = ds.schema
    targetSchema match {
      case targetSchema: StructType if sourceSchema == targetSchema => ds.toDF()
      case targetSchema: StructType =>
        reshapeStructType(sourceSchema, targetSchema, config, sourceColumn = null, normalizedPath = null) match {
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

  private def normalizeFieldName(field: StructField, config: ReshapeConfig): String = {
    if (config.normalizedNameMatching) {
      Strings.toSnakeCase(field.name.trim)
    } else {
      field.name.trim.toLowerCase
    }
  }

  private def reshapeStructField(sourceDataType: DataType, targetDataType: DataType, config: ReshapeConfig,
      sourceExpression: Expression): Either[List[ReshapeError], List[Column]] = {
    reshapeStructField(StructField(null, sourceDataType), StructField(null, targetDataType), config,
      new Column(sourceExpression), normalizedFieldPath = null)
  }

  private def reshapeStructField(sourceField: StructField, targetField: StructField, config: ReshapeConfig,
      sourceColumn: Column, normalizedFieldPath: String): Either[List[ReshapeError], List[Column]] = {
    val errors = mutable.Buffer.empty[ReshapeError]
    var column = getNestedColumn(sourceColumn, sourceField.name)
    if (sourceField.nullable && !targetField.nullable) {
      if (config.failOnIgnoringNullability) {
        errors += ReshapeError(normalizedFieldPath, "is nullable but target column is not")
      } else if (config.fillDefaultValues) {
        column = coalesce(column, new Column(defaultValueExpression(targetField.dataType)))
      }
    }
    (sourceField.dataType, targetField.dataType) match {
      case (sourceDataType, targetDataType) if sourceDataType == targetDataType => // pass
      case (sourceStructType: StructType, targetStructType: StructType) =>
        reshapeStructType(sourceStructType, targetStructType, config, column, normalizedFieldPath) match {
          case Right(nestedColumns) => column = struct(nestedColumns: _*)
          case Left(nestedErrors) => errors ++= nestedErrors
        }
      case (sourceArrayType: ArrayType, targetArrayType: ArrayType) =>
        val identifier = s"_${Strings.toWordCharactersCollapsing(normalizedFieldPath)}"
        val lambdaVariable = NamedLambdaVariable(identifier, sourceArrayType.elementType, sourceArrayType.containsNull)
        reshapeStructField(sourceArrayType.elementType, targetArrayType.elementType, config, lambdaVariable) match {
          case Right(nestedColumns) =>
            val lambdaFunction = LambdaFunction(nestedColumns.head.expr, Seq(lambdaVariable))
            column = new Column(ArrayTransform(column.expr, lambdaFunction))
          case Left(nestedErrors) =>
            errors ++= nestedErrors
        }
      case (sourceDataType, targetDataType) if config.coerceTypes && Cast.canCast(sourceDataType, targetDataType) =>
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
      sourceColumn: Column, normalizedPath: String): Either[List[ReshapeError], List[Column]] = {
    val sourceColumnPath = Option(sourceColumn).map(c => usePrettyExpression(c.expr).sql)

    val sourceFieldsByName = sourceStruct.fields.toList.groupBy(f => normalizeFieldName(f, config))
    val targetFieldsByName = targetStruct.fields.toList.groupBy(f => normalizeFieldName(f, config))

    val normalizedFieldNames = config.sortOrder match {
      case ReshapeConfig.SortOrder.ALPHABETICAL =>
        (sourceStruct.fields ++ targetStruct.fields).map(f => normalizeFieldName(f, config)).toSeq.distinct.sorted
      case ReshapeConfig.SortOrder.SOURCE | ReshapeConfig.SortOrder.UNDEFINED | null =>
        (sourceStruct.fields ++ targetStruct.fields).map(f => normalizeFieldName(f, config)).toSeq.distinct
      case ReshapeConfig.SortOrder.TARGET =>
        (targetStruct.fields ++ sourceStruct.fields).map(f => normalizeFieldName(f, config)).toSeq.distinct
    }

    val resultColumns = normalizedFieldNames.map { normalizedFieldName =>
      val normalizedFieldPath = sourceColumnPath.map(cp => s"$cp.$normalizedFieldName").getOrElse(normalizedFieldName)
      val sourceFields = sourceFieldsByName.getOrElse(normalizedFieldName, List.empty)
      val targetFields = targetFieldsByName.getOrElse(normalizedFieldName, List.empty)
      (sourceFields, targetFields) match {
        case (Nil, target :: Nil) =>
          if (target.nullable && config.fillMissingNulls) {
            Right(List(new Column(nullValueExpression(target.dataType)).as(target.name)))
          } else if (config.fillDefaultValues) {
            Right(List(new Column(defaultValueExpression(target.dataType)).as(target.name)))
          } else {
            Left(List(ReshapeError(normalizedFieldPath, "is missing")))
          }
        case (source :: Nil, target :: Nil) =>
          reshapeStructField(source, target, config, sourceColumn, normalizedFieldPath)
        case (sources, _ :: Nil) =>
          // TODO: ConflictResolution is more complicated than this
//        config.conflictResolution match {
//          case ReshapeConfig.ConflictResolution.ERROR | ReshapeConfig.ConflictResolution.UNDEFINED | null =>
//            Left(List(ReshapeError(normalizedFieldPath, s"has ${sources.size} conflicting occurrences")))
//          case ReshapeConfig.ConflictResolution.FIRST =>
//            reshapeStructField(sources.head, target, config, sourcePath, normalizedFieldPath)
//          case ReshapeConfig.ConflictResolution.LAST =>
//            reshapeStructField(sources.last, target, config, sourcePath, normalizedFieldPath)
//        }
          Left(List(ReshapeError(normalizedFieldPath, s"has ${sources.size} conflicting occurrences")))
        case (sources, Nil) =>
          if (config.failOnExtraColumn) {
            Left(List(ReshapeError(normalizedFieldPath, s"unexpectedly present (${sources.size}x)")))
          } else {
            if (config.dropExtraColumns) {
              Right(List.empty)
            } else {
              // TODO: Do we need ConflictResolution here?
              Right(sources.map(f => getNestedColumn(sourceColumn, f.name)))
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
}

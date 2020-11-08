package io.stoys.spark

import io.stoys.scala.Strings
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Cast, CreateArray, CreateMap, Expression, Literal}
import org.apache.spark.sql.functions.{array, coalesce, col, struct}
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
        reshapeStructType(sourceSchema, targetSchema, config, sourcePath = null, normalizedPath = null) match {
          case Left(errors) => throw ReshapeException(errors)
          case Right(columns) => ds.select(columns: _*)
        }
      case dt => throw new SparkException(s"Unsupported target schema '$dt'. Only StructType is supported.")
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

  private def makeFieldPath(path: String, fieldName: String): String = {
    Seq(path, fieldName).filter(_ != null).mkString(".")
  }

  private def normalizeFieldName(field: StructField, config: ReshapeConfig): String = {
    if (config.normalizedNameMatching) {
      Strings.toSnakeCase(field.name.trim)
    } else {
      field.name.trim.toLowerCase
    }
  }

  private def reshapeStructField(source: StructField, target: StructField, config: ReshapeConfig,
      sourcePath: String, normalizedFieldPath: String): Either[List[ReshapeError], List[Column]] = {
    val sourceFieldPath = makeFieldPath(sourcePath, source.name)

    val errors = mutable.Buffer.empty[ReshapeError]
    var column = col(sourceFieldPath)
    if (source.nullable && !target.nullable) {
      if (config.failOnIgnoringNullability) {
        errors += ReshapeError(normalizedFieldPath, "is nullable but target column is not")
      } else if (config.fillDefaultValues) {
        column = coalesce(column, new Column(defaultValueExpression(target.dataType)))
      }
    }
    (source.dataType, target.dataType) match {
      case (sourceDataType, targetDataType) if sourceDataType == targetDataType => // pass
      case (sourceStructType: StructType, targetStructType: StructType) =>
        reshapeStructType(sourceStructType, targetStructType, config, sourceFieldPath, normalizedFieldPath) match {
          case Right(nestedColumns) => column = struct(nestedColumns: _*)
          case Left(nestedErrors) => errors ++= nestedErrors
        }
      case (_: StructType, targetDataType) =>
        errors += ReshapeError(normalizedFieldPath, s"struct cannot be casted to target type $targetDataType")
      case (sourceDataType, _: StructType) if !sourceDataType.isInstanceOf[NullType] =>
        errors += ReshapeError(normalizedFieldPath, s"source type $sourceDataType cannot be casted to struct")
      case (sourceArrayType: ArrayType, targetArrayType: ArrayType) =>
        val sourceFieldType = StructField(null, sourceArrayType.elementType)
        val targetFieldType = StructField(null, targetArrayType.elementType)
        reshapeStructField(sourceFieldType, targetFieldType, config, sourceFieldPath, normalizedFieldPath) match {
          case Right(nestedColumns) =>
            assert(nestedColumns.size == 1)
            column = array(nestedColumns: _*)
          case Left(nestedErrors) =>
            errors ++= nestedErrors
        }
      case (_: ArrayType, targetDataType) =>
        errors += ReshapeError(normalizedFieldPath, s"array cannot be casted to target type $targetDataType")
      case (sourceDataType, _: ArrayType) if !sourceDataType.isInstanceOf[NullType] =>
        errors += ReshapeError(normalizedFieldPath, s"source type $sourceDataType cannot be casted to array")
      case (sourceDataType, targetDataType) =>
        if (Cast.canCast(sourceDataType, targetDataType) && config.coerceTypes) {
          column = column.cast(targetDataType)
        } else {
          errors += ReshapeError(normalizedFieldPath,
            s"type $sourceDataType cannot be casted to target type $targetDataType")
        }
    }

    if (errors.isEmpty) {
      Right(List(Option(target.name).map(tn => column.as(tn)).getOrElse(column)))
    } else {
      Left(errors.toList)
    }
  }

  private def reshapeStructType(sourceStruct: StructType, targetStruct: StructType, config: ReshapeConfig,
      sourcePath: String, normalizedPath: String): Either[List[ReshapeError], List[Column]] = {
    val sourceFieldsByName = sourceStruct.fields.toList.groupBy(f => normalizeFieldName(f, config))
    val targetFieldsByName = targetStruct.fields.toList.groupBy(f => normalizeFieldName(f, config))

    val normalizedFieldNames = config.sortOrder match {
      case ReshapeConfig.SortOrder.ALPHABETICAL =>
        (sourceStruct.fields ++ targetStruct.fields).map(f => normalizeFieldName(f, config)).toSeq.distinct.sorted
      case ReshapeConfig.SortOrder.SOURCE =>
        (sourceStruct.fields ++ targetStruct.fields).map(f => normalizeFieldName(f, config)).toSeq.distinct
      case ReshapeConfig.SortOrder.TARGET | ReshapeConfig.SortOrder.UNDEFINED =>
        (targetStruct.fields ++ sourceStruct.fields).map(f => normalizeFieldName(f, config)).toSeq.distinct
    }

    val resultColumns = normalizedFieldNames.map { normalizedFieldName =>
      val normalizedFieldPath = makeFieldPath(normalizedPath, normalizedFieldName)
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
          reshapeStructField(source, target, config, sourcePath, normalizedFieldPath)
        case (sources, _ :: Nil) =>
          // TODO: ConflictResolution is more complicated than this
//        config.conflictResolution match {
//          case ReshapeConfig.ConflictResolution.ERROR | ReshapeConfig.ConflictResolution.UNDEFINED =>
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
              Right(sources.map(f => col(makeFieldPath(sourcePath, f.name))))
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

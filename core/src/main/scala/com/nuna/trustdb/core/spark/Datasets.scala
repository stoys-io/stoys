package com.nuna.trustdb.core.spark

import com.nuna.trustdb.core.util.{Reflection, Strings}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Cast, CreateArray, CreateMap, Expression, Literal}
import org.apache.spark.sql.functions.{array, coalesce, col, struct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

object Datasets {
  /**
   * asDataset[T] is similar to spark's own as[T]
   *
   * But this one actually force the schema change to the underlying DataFrame (using projection - "select" statement).
   * The result is like as[T] but only having the fields specified in case class T and in the order they are in there.
   *
   * @param ds input [[Dataset]][T] or [[DataFrame]]
   * @tparam T case class to which we are casting the df
   * @return [[Dataset]][T] with only the columns present in case class and in that order
   */
  // TODO: Can we make this work for nested records?
  def asDataset[T <: Product : TypeTag](ds: Dataset[_]): Dataset[T] = {
    import ds.sparkSession.implicits._

    val actualColumns = ds.columns.toSeq
    val expectedColumns = Reflection.getCaseClassFieldNames[T]

    if (actualColumns == expectedColumns) {
      ds.as[T]
    } else {
      val missingColumns = expectedColumns.toSet -- actualColumns
      if (missingColumns.nonEmpty) {
        throw new RuntimeException(s"Data frame with columns $actualColumns is missing columns $missingColumns")
      }

      val duplicateColumns = actualColumns.diff(actualColumns.distinct).distinct
      if (duplicateColumns.nonEmpty) {
        throw new RuntimeException(s"Data frame has duplicate columns $duplicateColumns")
      }

      ds.select(expectedColumns.map(col): _*).as[T]
    }
  }

  case class Config(
      /**
       * Coerce types. It will automatically do the usual upcasting like [[Int]] to [[Long]] but not the other way
       * around (as it would loose precision). Beware of strings though! Everything can be casted implicitly
       * to [[String]] but it may not be what one intends to do.
       */
      coerceTypes: Boolean,
      /**
       * NOT IMPLEMENTED YET!!! It throws [[Config.ConflictResolution.ERROR]] regardless of the config (for now).
       *
       * What should happen when two conflicting column definitions are encountered? In particular the value
       * [[Config.ConflictResolution.LAST]] is useful. It solves the common pattern of conflicting columns occurring
       * in queries like "SELECT *, "some_value" AS value FROM table" when "table" already has "value".
       */
      conflictResolution: Config.ConflictResolution.ConflictingNameResolution,
      /**
       * Should we fail if we drop extra column (not present in target schema)?
       */
      failOnDroppingExtraColumn: Boolean,
      /**
       * Should we fill in default values instead of nulls for non nullable columns?
       *
       * Default values are 0 for numbers, false for [[Boolean]], "" for [[String]], empty arrays and nested struct
       * are present filled with the previous rules.
       *
       * Note: It probably does not make sense to use this without also setting fillMissingNulls = true.
       *
       * BEWARE: This is not intended for production code! Filling blindly all columns with default values defeats
       * the purpose of why they are not defined nullable in the first place. It would be error prone to use this.
       * But it may come handy in some notebook exploration or unit tests.
       */
      fillDefaultValues: Boolean,
      /**
       * Should we fill nulls instead of missing nullable columns?
       */
      fillMissingNulls: Boolean,
      /**
       * Should names be normalized before matching?
       *
       * Number of normalizations happen - lowercase, replace non-word characters with underscores, etc. For details
       * see [[Strings.toSnakeCase]].
       */
      normalizedNameMatching: Boolean,
      /**
       * How should the output columns be sorted? Use [[Config.SortOrder.TARGET]] to get the order of target schema.
       */
      sortOrder: Config.SortOrder.SortOrder,
      /**
       * Should we remove the extra columns (not present in target schema)? Use false to keep ghe extra columns
       * (just like [[Dataset.as]] does).
       */
      removeExtraColumns: Boolean,
  )

  object Config {
    object ConflictResolution extends Enumeration {
      type ConflictingNameResolution = Value
      val UNDEFINED, ERROR, FIRST, LAST = Value
    }

    object SortOrder extends Enumeration {
      type SortOrder = Value
      val UNDEFINED, ALPHABETICAL, SOURCE, TARGET = Value
    }

    val safe = Config(
      coerceTypes = false,
      conflictResolution = Config.ConflictResolution.ERROR,
      failOnDroppingExtraColumn = true,
      fillDefaultValues = false,
      fillMissingNulls = false,
      normalizedNameMatching = false,
      sortOrder = Config.SortOrder.SOURCE,
      removeExtraColumns = false,
    )
    val default = safe.copy(
      coerceTypes = true,
      failOnDroppingExtraColumn = false,
      sortOrder = Config.SortOrder.TARGET,
      removeExtraColumns = true,
    )
    val powerful = default.copy(
      conflictResolution = Config.ConflictResolution.LAST,
      fillMissingNulls = true,
      normalizedNameMatching = true,
    )
    val dangerous = powerful.copy(
      fillDefaultValues = true,
    )
  }

  case class StructValidationError(
      path: String,
      msg: String,
  )

  class StructValidationException(val errors: Seq[StructValidationError]) extends Exception {
    override def getMessage: String = {
      val errorStrings = errors.map(e => s"Column ${e.path} ${e.msg}")
      s"Errors: [${errorStrings.mkString("\n    - ", "\n    - ", "\n")}]"
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

  private def makeFiledPath(path: String, fieldName: String): String = {
    Seq(path, fieldName).filter(_ != null).mkString(".")
  }

  private def coarseStructField(source: StructField, target: StructField, config: Config,
      path: String): Either[List[StructValidationError], List[Column]] = {
    val fieldPath = makeFiledPath(path, target.name)

    val errors = mutable.Buffer.empty[StructValidationError]
    var column = col(makeFiledPath(path, source.name))
    if (source.nullable && !target.nullable) {
      if (config.fillDefaultValues) {
        column = coalesce(column, new Column(defaultValueExpression(target.dataType)))
      } else {
        errors += StructValidationError(fieldPath, "is nullable but target column is not")
      }
    }
    (source.dataType, target.dataType) match {
      case (sourceDataType, targetDataType) if sourceDataType == targetDataType => // pass
      case (sourceStructType: StructType, targetStructType: StructType) =>
        coarseStructType(sourceStructType, targetStructType, config, fieldPath) match {
          case Right(nestedColumns) => column = struct(nestedColumns: _*)
          case Left(nestedErrors) => errors ++= nestedErrors
        }
      case (_: StructType, targetDataType) =>
        errors += StructValidationError(fieldPath, s"struct cannot be casted to target type $targetDataType")
      case (sourceDataType, _: StructType) if !sourceDataType.isInstanceOf[NullType] =>
        errors += StructValidationError(fieldPath, s"source type $sourceDataType cannot be casted to struct")
      case (sourceArrayType: ArrayType, targetArrayType: ArrayType) =>
        val sourceFieldType = StructField(null, sourceArrayType.elementType)
        val targetFieldType = StructField(null, targetArrayType.elementType)
        coarseStructField(sourceFieldType, targetFieldType, config, fieldPath) match {
          case Right(nestedColumns) =>
            assert(nestedColumns.size == 1)
            column = array(nestedColumns: _*)
          case Left(nestedErrors) =>
            errors ++= nestedErrors
        }
      case (_: ArrayType, targetDataType) =>
        errors += StructValidationError(fieldPath, s"array cannot be casted to target type $targetDataType")
      case (sourceDataType, _: ArrayType) if !sourceDataType.isInstanceOf[NullType] =>
        errors += StructValidationError(fieldPath, s"source type $sourceDataType cannot be casted to array")
      case (sourceDataType, targetDataType) =>
        if (Cast.canCast(sourceDataType, targetDataType) && config.coerceTypes) {
          column = column.cast(targetDataType)
        } else {
          errors += StructValidationError(fieldPath,
            s"type $sourceDataType cannot be casted to target type $targetDataType")
        }
    }

    if (errors.isEmpty) {
      Right(List(Option(target.name).map(tn => column.as(tn)).getOrElse(column)))
    } else {
      Left(errors.toList)
    }
  }

  private def coarseStructType(sourceStruct: StructType, targetStruct: StructType, config: Config,
      path: String = null): Either[List[StructValidationError], List[Column]] = {
    def normalizedName(field: StructField): String = {
      // TODO: normalize even more? (leading and trailing spaces, etc)
      if (config.normalizedNameMatching) Strings.toSnakeCase(field.name) else field.name
    }

    val sourceFieldsByName = sourceStruct.fields.toList.groupBy(normalizedName)
    val targetFieldsByName = targetStruct.fields.toList.groupBy(normalizedName)

    val fieldNames = config.sortOrder match {
      case Config.SortOrder.ALPHABETICAL => (sourceFieldsByName.keys ++ targetFieldsByName.keys).toSeq.sorted
      case Config.SortOrder.SOURCE => (sourceStruct.fields ++ targetStruct.fields).map(normalizedName).toSeq.distinct
      case Config.SortOrder.TARGET | Config.SortOrder.UNDEFINED =>
        (targetStruct.fields ++ sourceStruct.fields).map(normalizedName).toSeq.distinct
    }

    val resultColumns = fieldNames.map { fieldName =>
      val fieldPath = makeFiledPath(path, fieldName)
      (sourceFieldsByName.getOrElse(fieldName, List.empty), targetFieldsByName.getOrElse(fieldName, List.empty)) match {
        case (Nil, target :: Nil) =>
          if (target.nullable && config.fillMissingNulls) {
            Right(List(new Column(nullValueExpression(target.dataType)).name(target.name)))
          } else if (config.fillDefaultValues) {
            Right(List(new Column(defaultValueExpression(target.dataType)).as(target.name)))
          } else {
            Left(List(StructValidationError(fieldPath, "is missing")))
          }
        case (source :: Nil, target :: Nil) =>
          coarseStructField(source, target, config, path)
        case (sources, target :: Nil) =>
          // TODO: ConflictResolution is more complicated than this
//          config.conflictResolution match {
//            case ConflictResolution.ERROR | ConflictResolution.UNDEFINED =>
//              Left(List(StructValidationError(fieldPath, s"has ${sources.size} conflicting occurrences")))
//            case ConflictResolution.FIRST => coarseStructField(sources.head, target, config, path)
//            case ConflictResolution.LAST => coarseStructField(sources.last, target, config, path)
//          }
          Left(List(StructValidationError(fieldPath, s"has ${sources.size} conflicting occurrences")))
        case (sources, Nil) =>
          if (config.failOnDroppingExtraColumn) {
            Left(List(StructValidationError(fieldPath, s"has been dropped (${sources.size}x)")))
          } else {
            if (config.removeExtraColumns) {
              Right(List.empty)
            } else {
              Right(List(col(fieldPath)))
            }
          }
        case (_, targets) if targets.size > 1 =>
          Left(List(StructValidationError(fieldPath, s"has ${targets.size} occurrences in target struct")))
      }
    }

    resultColumns.toList.partition(_.isLeft) match {
      case (Nil, columns) => Right(columns.flatMap(_.toOption).flatten)
      case (errors, _) => Left(errors.flatMap(_.left.toOption).flatten)
    }
  }

  /**
   * [[asDS]][T] is similar to spark's own [[Dataset.as]][T] but way more powerful and configurable
   *
   * See [[Config]] for supported configuration and/or unit tests for examples. Just be aware that the same config
   * is applied on all the columns (even nested).
   *
   * @param ds input [[Dataset]] or [[DataFrame]]
   * @param config configuration - see [[Config]] for details
   * @tparam T case class to which we are casting the df
   * @return [[Dataset]][T] with only the columns present in case class and in that order
   */
  def asDS[T <: Product : TypeTag](ds: Dataset[_], config: Config = Config.default): Dataset[T] = {
    import ds.sparkSession.implicits._

    val actualSchema = ds.schema
    val expectedSchema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

    if (actualSchema == expectedSchema) {
      ds.as[T]
    } else {
      coarseStructType(actualSchema, expectedSchema, config) match {
        case Left(errors) => throw new StructValidationException(errors)
        case Right(columns) => ds.select(columns: _*).as[T]
      }
    }
  }

  class RichDataset(val ds: Dataset[_]) extends AnyVal {
    def asDataset[T <: Product : TypeTag]: Dataset[T] = {
      Datasets.asDataset[T](ds)
    }

    def asDS[T <: Product : TypeTag]: Dataset[T] = {
      Datasets.asDS[T](ds)
    }

    def asDS[T <: Product : TypeTag](config: Config): Dataset[T] = {
      Datasets.asDS[T](ds, config)
    }
  }
}

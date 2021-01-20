package io.stoys.scala

import scala.reflect.api
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

// READ THIS BEFORE USE!!!
//
// In order to avoid issues remember to use:
//   * `cleanupReflection` to wrap any call of `isSubtype` (or `<:<`)  (to clean up scala reflection memory)
//   * `isSubtype` instead of `<:<` (for thread safety)
//   * `localTypeOf` instead of `typeOf` (to avoid classloader issues in some ide and notebooks)
//   * `baseType` before doing anything ith reflection on a type
//
// Note: These workaround functions here are taken from `org.apache.spark.sql.catalyst.ScalaReflection`.
//       Take a look there for better description and credits. Kudos to Apache Spark contributors!
object Reflection {
  private object ReflectionSubtypeLock

  private def mirror: universe.Mirror = {
    universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
  }

  /**
   * Wrapper for [[isSubtype]] (`<:<` operator) (clean up scala reflection memory)
   *
   * @see `org.apache.spark.sql.catalyst.ScalaReflection.cleanUpReflectionObjects`
   */
  def cleanupReflection[T](body: => T): T = {
    universe.asInstanceOf[scala.reflect.runtime.JavaUniverse].undoLog.undo(body)
  }

  /**
   * Thread safe replacement of [[<:<]] operator
   *
   * @see `org.apache.spark.sql.catalyst.ScalaReflection.isSubtype`
   */
  def isSubtype(tpe1: Type, tpe2: Type): Boolean = {
    ReflectionSubtypeLock.synchronized {
      tpe1 <:< tpe2
    }
  }

  /**
   * Return type [[T]] in the current classloader mirror (avoid classloader issues in ide and notebook)
   *
   * @see `org.apache.spark.sql.catalyst.ScalaReflection.localTypeOf`
   */
  def localTypeOf[T: TypeTag]: Type = {
    baseType(typeTag[T].in(mirror).tpe).dealias
  }

  /**
   * Return base type (removing things like aliases and annotation)
   *
   * @see `org.apache.spark.sql.catalyst.ScalaReflection.baseType`
   */
  def baseType(tpe: Type): Type = {
    tpe.dealias match {
      case annotatedType: AnnotatedType => annotatedType.underlying
      case other => other
    }
  }

  def typeSymbolOf(tpe: Type): TypeSymbol = {
    baseType(tpe).typeSymbol.asType
  }

  def typeSymbolOf[T: TypeTag]: TypeSymbol = {
    typeSymbolOf(localTypeOf[T])
  }

  def nameOf(symbol: Symbol): String = {
    symbol.name.decodedName.toString
  }

  def typeNameOf(tpe: Type): String = {
    nameOf(typeSymbolOf(tpe))
  }

  def typeNameOf(symbol: Symbol): String = {
    typeNameOf(symbol.typeSignature)
  }

  def typeNameOf[T: TypeTag]: String = {
    typeNameOf(localTypeOf[T])
  }

  def fullTypeNameOf(tpe: Type): String = {
    typeSymbolOf(tpe).fullName
  }

  def fullTypeNameOf(symbol: Symbol): String = {
    fullTypeNameOf(symbol.typeSignature)
  }

  def fullTypeNameOf[T: TypeTag]: String = {
    fullTypeNameOf(localTypeOf[T])
  }

  def isCaseClass(symbol: Symbol): Boolean = {
    symbol.isClass && symbol.asClass.isCaseClass
  }

  def isCaseClass(tpe: Type): Boolean = {
    isCaseClass(typeSymbolOf(tpe))
  }

  def isCaseClass[T: TypeTag]: Boolean = {
    isCaseClass(typeSymbolOf[T])
  }

  def isAnnotated(symbol: Symbol, aTpe: Type): Boolean = {
    symbol.annotations.exists(_.tree.tpe =:= aTpe)
  }

  def isAnnotated[A: TypeTag](symbol: Symbol): Boolean = {
    isAnnotated(symbol, localTypeOf[A])
  }

  def isAnnotated[T: TypeTag, A: TypeTag]: Boolean = {
    isAnnotated(typeSymbolOf[T], localTypeOf[A])
  }

  def assertCaseClass(symbol: Symbol): Unit = {
    if (!isCaseClass(symbol)) {
      throw new IllegalArgumentException(s"${fullTypeNameOf(symbol)} is not a case class!")
    }
  }

  def assertCaseClass(tpe: Type): Unit = {
    assertCaseClass(typeSymbolOf(tpe))
  }

  def assertCaseClass[T: TypeTag](): Unit = {
    assertCaseClass(typeSymbolOf[T])
  }

  private def assertAnnotated(symbol: Symbol, aTpe: Type): Unit = {
    if (!isAnnotated(symbol, aTpe)) {
      throw new IllegalArgumentException(s"${fullTypeNameOf(symbol)} is not annotated with ${fullTypeNameOf(aTpe)}!")
    }
  }

  def assertAnnotated[A: TypeTag](symbol: Symbol): Unit = {
    assertAnnotated(symbol, localTypeOf[A])
  }

  def assertAnnotated[A: TypeTag](tpe: Type): Unit = {
    assertAnnotated(typeSymbolOf(tpe), localTypeOf[A])
  }

  def assertAnnotated[T: TypeTag, A: TypeTag](): Unit = {
    assertAnnotated(typeSymbolOf[T], localTypeOf[A])
  }

  private def assertAnnotatedCaseClass(symbol: Symbol, aTpe: Type): Unit = {
    assertCaseClass(symbol)
    assertAnnotated(symbol, aTpe)
  }

  def assertAnnotatedCaseClass[A: TypeTag](symbol: Symbol): Unit = {
    assertAnnotatedCaseClass(symbol, localTypeOf[A])
  }

  def assertAnnotatedCaseClass[A: TypeTag](tpe: Type): Unit = {
    assertAnnotatedCaseClass(typeSymbolOf(tpe), localTypeOf[A])
  }

  def assertAnnotatedCaseClass[T: TypeTag, A: TypeTag](): Unit = {
    assertAnnotatedCaseClass(typeSymbolOf[T], localTypeOf[A])
  }

  def getCaseClassFields(tpe: Type): Seq[Symbol] = {
    assertCaseClass(tpe)
    typeSymbolOf(tpe).asClass.primaryConstructor.asMethod.paramLists.flatten
  }

  def getCaseClassFields[T: TypeTag]: Seq[Symbol] = {
    getCaseClassFields(localTypeOf[T])
  }

  def getCaseClassFieldNames[T: TypeTag]: Seq[String] = {
    getCaseClassFields[T].map(nameOf)
  }

  def createCaseClassInstance(tpe: Type, args: Seq[Any]): Any = {
    val applyMethod = tpe.companion.decl(TermName("apply")).asMethod
    val obj = mirror.reflectModule(tpe.typeSymbol.companion.asModule).instance
    mirror.reflect(obj).reflectMethod(applyMethod)(args: _*)
  }

  def createCaseClassInstance[T: TypeTag](args: Seq[Any]): T = {
    createCaseClassInstance(localTypeOf[T], args).asInstanceOf[T]
  }

  def enumerationValuesOf(tpe: Type): Seq[Enumeration#Value] = {
    val parentType = tpe.asInstanceOf[TypeRef].pre
    val valuesMethod = parentType.baseType(localTypeOf[Enumeration].typeSymbol).decl(TermName("values")).asMethod
    val obj = mirror.reflectModule(parentType.termSymbol.asModule).instance
    val valueSet = mirror.reflect(obj).reflectMethod(valuesMethod)().asInstanceOf[Enumeration#ValueSet]
    valueSet.toSeq
  }

  def enumerationValuesOf[E <: Enumeration#Value : TypeTag]: Seq[E] = {
    enumerationValuesOf(localTypeOf[E]).asInstanceOf[Seq[E]]
  }

  def getFieldValue(obj: Product, fieldName: String): Any = {
    try {
      val field = obj.getClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      field.get(obj)
    } catch {
      case _: NoSuchFieldException =>
        throw new NoSuchFieldException(s"Field '$fieldName' not found in class ${obj.getClass}!")
    }
  }

  private def getAnnotationParams(annotation: Annotation): Seq[(String, Any)] = {
    def getAnnotationParams(tree: Tree): Seq[(String, Any)] = {
      tree match {
        case Apply(Select(New(TypeTree()), termNames.CONSTRUCTOR), valueTrees) =>
          valueTrees.map {
            case AssignOrNamedArg(Ident(TermName(key)), valueTree) => key -> getValue(valueTree)
          }
      }
    }

    def getValue(tree: Tree): Any = {
      tree match {
        // enum
        case Literal(Constant(value: TermSymbol)) =>
          val clazz = mirror.runtimeClass(value.owner.asClass)
          val valueName = value.name.toString
          clazz.getMethod("valueOf", classOf[String]).invoke(null, valueName)
        // classOf[...]
        case Literal(Constant(value: TypeRef)) => mirror.runtimeClass(value)
        // (boxed) primitive and string
        case Literal(Constant(value)) => value
        // array
        case Apply(Ident(TermName("Array")), valueTrees) => valueTrees.map(getValue)
        // annotation
        case tree@Apply(Select(New(TypeTree()), termNames.CONSTRUCTOR), _) => getAnnotationParams(tree)
      }
    }

    getAnnotationParams(annotation.tree)
  }

  def getAnnotationParams[A: TypeTag](symbol: Symbol): Option[Seq[(String, Any)]] = {
    symbol.annotations.find(_.tree.tpe =:= localTypeOf[A]).map(getAnnotationParams)
  }

  def getAnnotationParams[T: TypeTag, A: TypeTag]: Option[Seq[(String, Any)]] = {
    getAnnotationParams[A](typeSymbolOf[T])
  }

  def getAnnotationParamsMap[A: TypeTag](symbol: Symbol): Map[String, Any] = {
    getAnnotationParams[A](symbol).getOrElse(Seq.empty).toMap
  }

  def getAnnotationParamsMap[T: TypeTag, A: TypeTag]: Map[String, Any] = {
    getAnnotationParamsMap[A](typeSymbolOf[T])
  }

  def getAllAnnotationsParamsMap(symbol: Symbol): Map[String, Map[String, Any]] = {
    symbol.annotations.map(a => fullTypeNameOf(a.tree.tpe) -> getAnnotationParams(a).toMap).toMap
  }

  private def renderAnnotation(annotation: Annotation): String = {
    val renderedParams = getAnnotationParams(annotation).map {
      case (name, value: String) => s"""$name = "$value""""
      case (name, value) => s"$name = $value"
    }
    s"@${typeNameOf(annotation.tree.tpe)}${renderedParams.mkString("(", ", ", ")")}"
  }

  def renderAnnotatedType(tpe: Type): String = {
    s"${typeSymbolOf(tpe).annotations.map(renderAnnotation).mkString(" ")} ${typeNameOf(tpe)}".trim
  }

  def renderAnnotatedType[T: TypeTag]: String = {
    renderAnnotatedType(localTypeOf[T])
  }

  private def typeTagOf[T](tpe: Type): TypeTag[T] = {
    TypeTag(mirror, new api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]): U#Type = {
        assert(m.eq(mirror), s"TypeTag[$tpe] defined in $mirror cannot be migrated to $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
  }

  // TODO: Remove this or at least make it private.
  def classNameToTypeTag(fullClassName: String): TypeTag[_] = {
    // Workaround for classes inside objects (`package.ObjectName$ClassName` => `package.ObjectName.ClassName`).
    val cleanFullClassName = fullClassName.replace('$', '.')
    typeTagOf(appliedType(mirror.staticClass(cleanFullClassName)))
  }

  def copyCaseClass[T <: Product](originalValue: T, map: Map[String, Any]): T = {
    val clazz = originalValue.getClass
    val fields = clazz.getDeclaredFields
    val copyMethod = clazz.getMethod("copy", fields.map(_.getType): _*)
    // TODO: Should we use getter methods instead?
    val args = fields.zip(originalValue.productIterator.toArray.asInstanceOf[Array[AnyRef]]).map {
      case (field, originalValue) => map.getOrElse(field.getName, originalValue).asInstanceOf[AnyRef]
    }
    copyMethod.invoke(originalValue, args: _*).asInstanceOf[T]
  }
}

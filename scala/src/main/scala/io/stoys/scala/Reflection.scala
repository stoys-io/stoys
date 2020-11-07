package io.stoys.scala

import scala.reflect.api
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

object Reflection {
  private def mirror: universe.Mirror = {
    universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
  }

  def dealiasedTypeOf[T: TypeTag]: Type = {
    typeOf[T].dealias
  }

  def dealiasedTypeSymbolOf(symbol: Symbol): Symbol = {
    symbol.typeSignature.dealias.typeSymbol
  }

  def dealiasedTypeSymbolOf[T: TypeTag]: Symbol = {
    dealiasedTypeOf[T].typeSymbol
  }

  def termNameOf(symbol: Symbol): String = {
    symbol.name.decodedName.toString
  }

  def typeNameOf(symbol: Symbol): String = {
    dealiasedTypeSymbolOf(symbol).name.decodedName.toString
  }

  def typeNameOf(tpe: Type): String = {
    typeNameOf(tpe.typeSymbol)
  }

  def typeNameOf[T: TypeTag]: String = {
    typeNameOf(symbolOf[T])
  }

  def isCaseClass(symbol: Symbol): Boolean = {
    val dealiasedTypeSymbol = dealiasedTypeSymbolOf(symbol)
    dealiasedTypeSymbol.isClass && dealiasedTypeSymbol.asClass.isCaseClass
  }

  def isCaseClass(tpe: Type): Boolean = {
    isCaseClass(tpe.typeSymbol)
  }

  def isCaseClass[T: TypeTag]: Boolean = {
    isCaseClass(symbolOf[T])
  }

  def isAnnotated[T: TypeTag, A: TypeTag]: Boolean = {
    dealiasedTypeSymbolOf[T].annotations.exists(_.tree.tpe =:= typeOf[A])
  }

  def assertCaseClass[T: TypeTag](): Unit = {
    if (!isCaseClass[T]) {
      throw new IllegalArgumentException(s"${symbolOf[T].fullName} is not a case class!")
    }
  }

  def assertAnnotated[T: TypeTag, A: TypeTag](): Unit = {
    if (!isAnnotated[T, A]) {
      throw new IllegalArgumentException(s"${symbolOf[T].fullName} is not annotated with ${symbolOf[A].fullName}!")
    }
  }

  def assertAnnotatedCaseClass[T: TypeTag, A: TypeTag](): Unit = {
    assertCaseClass[T]()
    assertAnnotated[T, A]()
  }

  def getCaseClassFields[T: TypeTag]: Seq[Symbol] = {
    assertCaseClass[T]()
    dealiasedTypeSymbolOf[T].asClass.primaryConstructor.typeSignature.paramLists.head
  }

  def getCaseClassFieldNames[T: TypeTag]: Seq[String] = {
    getCaseClassFields[T].map(_.name.decodedName.toString)
  }

  def createCaseClassInstance[T: TypeTag](args: Seq[Any]): T = {
    val dealiasedType = dealiasedTypeOf[T]
    val applyMethod = dealiasedType.companion.decl(TermName("apply")).asMethod
    val obj = mirror.reflectModule(dealiasedType.typeSymbol.companion.asModule).instance
    mirror.reflect(obj).reflectMethod(applyMethod)(args: _*).asInstanceOf[T]
  }

  def getEnumerationValues[E <: Enumeration : TypeTag]: Seq[E#Value] = {
    val parentType = dealiasedTypeOf[E].asInstanceOf[TypeRef].pre
    val valuesMethod = parentType.baseType(typeOf[Enumeration].typeSymbol).decl(TermName("values")).asMethod
    val obj = mirror.reflectModule(parentType.termSymbol.asModule).instance
    val valueSet = mirror.reflect(obj).reflectMethod(valuesMethod)().asInstanceOf[E#ValueSet]
    valueSet.toSeq
  }

  def getFieldValue(params: Product, fieldName: String): Any = {
    try {
      val field = params.getClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      field.get(params)
    } catch {
      case _: NoSuchFieldException =>
        throw new NoSuchFieldException(s"Field '$fieldName' not found in class ${params.getClass}!")
    }
  }

  def getAnnotationParams(annotation: Annotation): Seq[(String, Any)] = {
    def getAnnotationParams(tree: Tree): Seq[(String, Any)] = tree match {
      case Apply(Select(New(TypeTree()), termNames.CONSTRUCTOR), valueTrees) =>
        valueTrees.map {
          case AssignOrNamedArg(Ident(TermName(key)), valueTree) => key -> getValue(valueTree)
        }
    }

    def getValue(tree: Tree): Any = tree match {
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
      case tree @ Apply(Select(New(TypeTree()), termNames.CONSTRUCTOR), _) => getAnnotationParams(tree)
    }

    getAnnotationParams(annotation.tree)
  }

  def getAnnotationParams[A: TypeTag](symbol: Symbol): Option[Seq[(String, Any)]] = {
//    dealiasTypeOf(symbol).annotations.find(_.tree.tpe =:= typeOf[A]).map(getAnnotationParams)
    symbol.annotations.find(_.tree.tpe =:= typeOf[A]).map(getAnnotationParams)
  }

  def renderAnnotation(annotation: Annotation): String = {
    val renderedParams = getAnnotationParams(annotation).map {
      case (name, value: String) => s"""$name = "$value""""
      case (name, value) => s"$name = $value"
    }
    s"@${typeNameOf(annotation.tree.tpe.typeSymbol)}${renderedParams.mkString("(", ", ", ")")}"
  }

  def renderAnnotations(symbol: Symbol): String = {
    dealiasedTypeSymbolOf(symbol).annotations.map(renderAnnotation).mkString(" ")
  }

  def renderAnnotatedSymbol(symbol: Symbol): String = {
    s"${renderAnnotations(symbol)} ${typeNameOf(symbol)}".trim
  }

  def typeToTypeTag[T](tpe: Type): TypeTag[T] = {
    TypeTag(mirror, new api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]): U#Type = {
        assert(m.eq(mirror), s"TypeTag[$tpe] defined in $mirror cannot be migrated to $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
  }

  def classNameToTypeTag(fullClassName: String): TypeTag[_] = {
    // Workaround for classes inside objects (`package.ObjectName$ClassName` => `package.ObjectName.ClassName`).
    val cleanFullClassName = fullClassName.replace('$', '.')
    typeToTypeTag(appliedType(mirror.staticClass(cleanFullClassName)))
  }
}

package io.stoys.scala

import org.apache.commons.io.IOUtils

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

object IO {
  def resourceToStringOption(clazz: Class[_], fileName: String): Option[String] = {
    val resourceInputStream = Option(clazz.getResourceAsStream(fileName))
    resourceInputStream.map(ris => using(ris)(ris => IOUtils.toString(ris, StandardCharsets.UTF_8)))
  }

  def resourceToString(clazz: Class[_], fileName: String): String = {
    resourceToStringOption(clazz, fileName) match {
      case Some(content) => content
      case None => throw new IOException(s"File $fileName not found in package ${clazz.getPackage.getName}!")
    }
  }

  def resourceToByteArrayOption(clazz: Class[_], fileName: String): Option[Array[Byte]] = {
    val resourceInputStream = Option(clazz.getResourceAsStream(fileName))
    resourceInputStream.map(ris => using(ris)(ris => IOUtils.toByteArray(ris)))
  }

  def resourceToByteArray(clazz: Class[_], fileName: String): Array[Byte] = {
    resourceToByteArrayOption(clazz, fileName) match {
      case Some(content) => content
      case None => throw new IOException(s"File $fileName not found in package ${clazz.getPackage.getName}!")
    }
  }

  def using[R <: AutoCloseable, T](resource: R)(body: R => T): T = {
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      body(resource)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  private def closeAndAddSuppressed(primaryException: Throwable, resource: AutoCloseable): Unit = {
    if (primaryException != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressedException) =>
          primaryException.addSuppressed(suppressedException)
      }
    } else {
      resource.close()
    }
  }
}

package com.nuna.trustdb.core.util

import java.io.IOException
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils

import scala.util.control.NonFatal

object IO {
  def safeReadResource(clazz: Class[_], fileName: String): Option[String] = {
    val resourceInputStream = Option(clazz.getResourceAsStream(fileName))
    resourceInputStream.map(ris => using(ris)(ris => IOUtils.toString(ris, StandardCharsets.UTF_8.name())))
  }

  def readResource(clazz: Class[_], fileName: String): String = {
    val resourceInputStream = clazz.getResourceAsStream(fileName)
    if (resourceInputStream == null) {
      throw new IOException(s"File $fileName not found in package ${clazz.getPackage.getName}!")
    }
    using(resourceInputStream)(ris => IOUtils.toString(ris, StandardCharsets.UTF_8.name()))
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

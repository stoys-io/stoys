package io.stoys.spark

import io.stoys.scala.Reflection

import java.util.{List => JList, Map => JMap}
import scala.jdk.CollectionConverters._

object Py4j {
  val emptyOption: Option[Nothing] = None
  val emptySeq: Seq[Nothing] = Seq.empty
  val emptyMap: Map[Nothing, Nothing] = Map.empty

  def toArray[T](list: JList[T]): Array[T] = {
    list.toArray().asInstanceOf[Array[T]]
  }

  def toList[T](list: JList[T]): List[T] = {
    list.asScala.toList
  }

  def toMap[K, V](map: JMap[K, V]): Map[K, V] = {
    map.asScala.toMap
  }

  def toOption[T](value: T): Option[T] = {
    Option(value)
  }

  def toSeq[T](list: JList[T]): Seq[T] = {
    list.asScala.toSeq
  }

  def toSet[T](list: JList[T]): Set[T] = {
    list.asScala.toSet
  }

  def copyCaseClass[T <: Product, V](originalValue: T, map: JMap[String, V]): T = {
    Reflection.copyCaseClass(originalValue, toMap(map))
  }
}

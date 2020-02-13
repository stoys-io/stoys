package com.nuna.trustdb.core.util

object Strings {
  private val PARAM_PATTERN = """\$\{([^\}]*)\}""".r("param")

  def replaceParams(text: String, params: Option[Product]): String = {
    params.map(p => PARAM_PATTERN.replaceAllIn(text, m => Reflection.getFieldValue(p, m.group("param")).toString))
        .getOrElse(text)
  }

  def replaceParams(text: String, params: Map[String, Any]): String = {
    PARAM_PATTERN.replaceAllIn(text, m => params(m.group("param")).toString)
  }

  def toSnakeCase(value: String): String = {
    val charactersWithUnderscores = s"_${value}_".toCharArray.sliding(3).flatMap {
      case Array(_, c, _) if c.isWhitespace => Array('_')
      case Array(p, c, _) if p.isWhitespace => Array(c)
      case Array('_', c, _) => Array(c)
      case Array(p, c, _) if p.isLower && c.isUpper => Array('_', c)
      case Array(_, c, n) if c.isUpper && n.isLower => Array('_', c)
      case Array(_, c, _) => Array(c)
    }
    charactersWithUnderscores.mkString.toLowerCase()
  }

  def trim(value: String): Option[String] = {
    Option(value).map(_.trim).filterNot(_.isEmpty)
  }

  def toSqlListLiterals(values: Seq[String]): String = {
    values.map(v => "\"" + v + "\"").mkString("(", ",", ")")
  }

  def toWordCharacters(value: String): String = {
    value.replaceAll("\\W", "_")
  }

  def toWordCharactersCollapsing(value: String): String = {
    value.replaceAll("\\W[_\\W]*", "_")
  }

  // BEWARE: This function does not understand language syntax nor escaping! It just splits at the marker.
  def unsafeRemoveLineComments(value: String, marker: String): String = {
    value.split('\n').map(_.split(marker).head).mkString("\n")
  }
}

package io.stoys.spark

import org.apache.spark.sql.catalyst.util.quoteIdentifier

object SqlUtils {
  private val UNQUOTE_SAFE_IDENTIFIER_PATTERN = "^([a-zA-Z_][a-zA-Z0-9_]*)$".r("identifier")

  def quoteIfNeeded(part: String): String = {
    part match {
      case UNQUOTE_SAFE_IDENTIFIER_PATTERN(_) => part
      case p => quoteIdentifier(p)
    }
  }
}

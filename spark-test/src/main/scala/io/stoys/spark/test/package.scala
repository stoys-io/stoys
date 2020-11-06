package io.stoys.spark

import java.sql.Date
import java.time.LocalDate

package object test {
  // BEWARE: Dangerous and powerful implicits lives here! Be careful what we add here.
  // Do NOT copy this to src/main! It does not belong to production code. It is for tests only!
  object implicits {
    import _root_.scala.language.implicitConversions

    implicit def toOption[T](value: T): Option[T] = Some(value)

    implicit def toDate(date: String): Date = Date.valueOf(date)

    implicit def toLocalDate(date: String): LocalDate = LocalDate.parse(date)
  }
}

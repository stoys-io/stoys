package io.stoys.spark

case class ReshapeError(
    path: String,
    msg: String
)

class ReshapeException(val errors: Seq[ReshapeError])
    extends Exception(ReshapeException.errorsToMessage(errors)) {
}

object ReshapeException {
  def apply(errors: Seq[ReshapeError]): ReshapeException = {
    new ReshapeException(errors)
  }

  private def errorsToMessage(errors: Seq[ReshapeError]): String = {
    val errorStrings = errors.map(e => s"Column ${e.path} ${e.msg}")
    s"Errors: [${errorStrings.mkString("\n    - ", "\n    - ", "\n")}]"
  }
}

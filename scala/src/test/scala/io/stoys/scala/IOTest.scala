package io.stoys.scala

import org.scalatest.funsuite.AnyFunSuite

import java.io.IOException
import java.nio.charset.StandardCharsets

class IOTest extends AnyFunSuite {
  import IOTest._

  test("resourceToString*") {
    val content = IO.resourceToString(this.getClass, "test_resource.txt")
    assert(content === "test resource content\n")
    assertThrows[IOException](IO.resourceToString(this.getClass, "non_existing_resource.txt"))
  }

  test("resourceToByteArray*") {
    val content = IO.resourceToByteArray(this.getClass, "test_resource.txt")
    assert(content === "test resource content\n".getBytes(StandardCharsets.UTF_8.toString))
    assertThrows[IOException](IO.resourceToByteArray(this.getClass, "non_existing_resource.txt"))
  }

  test("using") {
    val resource = new TestResource()
    assert(!resource.closeCalled)
    IO.using(resource)(_ => ())
    assert(resource.closeCalled)
  }
}

object IOTest {
  class TestResource(var closeCalled: Boolean = false) extends AutoCloseable {
    override def close(): Unit = {
      closeCalled = true
    }
  }
}

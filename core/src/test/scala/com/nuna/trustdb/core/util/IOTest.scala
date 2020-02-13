package com.nuna.trustdb.core.util

import java.io.IOException

import org.scalatest.funsuite.AnyFunSuite

class IOTest extends AnyFunSuite {
  import IOTest._

  test("readResource") {
    val content = IO.readResource(this.getClass, "test_resource.txt")
    assert(content === "test resource content\n")
    assertThrows[IOException] {
      IO.readResource(this.getClass, "non_existing_resource.txt")
    }
  }

  test("using") {
    val resource = new TestResource()
    assert(!resource.closeCalled)
    IO.using(resource) { _ => Unit }
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

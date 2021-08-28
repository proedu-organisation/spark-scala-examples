package scala_mock

import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import traits.DemoHelper

class DemoTest extends AnyFunSuite with MockFactory {
  test("test_1") {
    val demoHelperMock = stub[DemoHelper]
    (demoHelperMock.saveToDB _).when().returns(20)

    val x = new Demo
    print(x.doStuff)
  }
}

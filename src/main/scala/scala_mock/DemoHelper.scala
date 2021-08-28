package scala_mock

class DemoHelper extends traits.DemoHelper {

  def saveToDB(): Int = {
    10
  }
}

object DemoHelper {
  def apply(): DemoHelper = {
    val p = new DemoHelper
    p
  }

  def apply(mock: DemoHelper): DemoHelper = {
    val p = mock
    p
  }

}

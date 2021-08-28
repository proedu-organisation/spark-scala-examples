package scala_mock

class Demo {
  val demoHelper = DemoHelper()
  def doStuff(): Unit ={
    val x = demoHelper.saveToDB()
    print(x)
  }
}

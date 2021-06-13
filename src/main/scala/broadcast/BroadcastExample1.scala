package broadcast

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastExample1 extends App {

  // Creating SparkConf.
  val conf = new SparkConf
  conf.setMaster("local[4]")
  conf.setAppName("broadcast test")

  // Creating SparkCOntext.
  val sc = new SparkContext(conf)

  // Set log level to DEBUG.
  sc.setLogLevel("DEBUG")

  // Creating broadcast variable.
  val broadcast = sc.broadcast(Seq(1, 2, 3, 4, 5))

  // Reading value of a broadcast variable.
  println(broadcast.value)
}

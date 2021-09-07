package rdd.transformations

import org.apache.spark.sql.SparkSession
import rdd.transformations.MapExample2.{file, sparkContext}

object FilterExample2 extends App {
  val file = "src/main/resources/retail_db/orders"

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  // Reading orders data as an RDD.
  val ordersRDD = sparkContext.textFile(file)
  ordersRDD.take(10).foreach(println)

  /** Output:
   * 1,2013-07-25 00:00:00.0,11599,CLOSED
   * 2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
   * 3,2013-07-25 00:00:00.0,12111,COMPLETE
   * 4,2013-07-25 00:00:00.0,8827,CLOSED
   * 5,2013-07-25 00:00:00.0,11318,COMPLETE
   */

  val completedOrders = ordersRDD.filter(record => record.contains("COMPLETE"))
  completedOrders.collect.foreach(println)

  /** Output showing only COMPLETE orders.
   * 68742,2013-10-31 00:00:00.0,197,COMPLETE
   * 68753,2013-11-15 00:00:00.0,6927,COMPLETE
   * 68755,2013-11-17 00:00:00.0,2644,COMPLETE
   * 68759,2013-11-27 00:00:00.0,16,COMPLETE
   * 68760,2013-11-30 00:00:00.0,3603,COMPLETE
   */

}

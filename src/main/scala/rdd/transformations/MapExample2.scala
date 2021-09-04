package rdd.transformations

import org.apache.spark.sql.SparkSession

object MapExample2 extends App {
  val file = "src/main/resources/retail_db/orders"

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  val ordersRDD = sparkContext.textFile(file)
  ordersRDD.take(10).foreach(println)

  /** Output:
   * 1,2013-07-25 00:00:00.0,11599,CLOSED
   * 2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
   * 3,2013-07-25 00:00:00.0,12111,COMPLETE
   * 4,2013-07-25 00:00:00.0,8827,CLOSED
   * 5,2013-07-25 00:00:00.0,11318,COMPLETE
   */

  val statusRDD = ordersRDD.map(order => order.split(",")(3))
  statusRDD.collect.foreach(println)

  /** Output
   * COMPLETE
   * COMPLETE
   * COMPLETE
   * PENDING
   * PENDING_PAYMENT
   * PROCESSING
   * CLOSED
   */

  val distinctRDD = statusRDD.distinct()
  distinctRDD.collect.foreach(println)

  /** Output
   * PENDING_PAYMENT
   * CLOSED
   * CANCELED
   * PAYMENT_REVIEW
   * PENDING
   * ON_HOLD
   * PROCESSING
   * SUSPECTED_FRAUD
   * COMPLETE
   */

}

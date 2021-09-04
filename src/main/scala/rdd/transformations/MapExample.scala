package rdd.transformations

import org.apache.spark.sql.SparkSession

object MapExample extends App {

  // Preparing the data for Map
  val days = List("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  val daysRDD = sparkContext.parallelize(days)
  daysRDD.collect.foreach(println)

  /**Sunday
  Monday
  Tuesday
  Wednesday
  Thursday
  Friday
  Saturday*/

  val tupleRDD = daysRDD.map(day => (day,day.length))
  tupleRDD.collect.foreach(println)

  /**
   * (Sunday,6)
   * (Monday,6)
   * (Tuesday,7)
   * (Wednesday,9)
   * (Thursday,8)
   * (Friday,6)
   * (Saturday,8)
   */

}

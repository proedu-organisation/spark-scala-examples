package rdd.transformations

import org.apache.spark.sql.SparkSession

object MapPartitionsExample extends App {

  // Preparing the data for Map
  val days = List("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  val daysRDD = sparkContext.parallelize(days)

  // Let's print some data.
  daysRDD.collect.foreach(println)

  /** Output
   * Sunday
   * Monday
   * Tuesday
   * Wednesday
   * Thursday
   * Friday
   * Saturday
   */

  // Using mapPartitions transformation.
  val mapPartitionRDD = daysRDD.mapPartitions(iterator => {
    val list = iterator.toList
    val tupleList = list.map(word => (word, word.length))
    tupleList.iterator
  })

  // Let's print some data.
  mapPartitionRDD.collect.foreach(println)

  /** Output
   * (Sunday,6)
   * (Monday,6)
   * (Tuesday,7)
   * (Wednesday,9)
   * (Thursday,8)
   * (Friday,6)
   * (Saturday,8)
   */
}

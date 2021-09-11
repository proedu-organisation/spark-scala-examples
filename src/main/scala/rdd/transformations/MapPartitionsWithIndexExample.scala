package rdd.transformations

import org.apache.spark.sql.SparkSession

object MapPartitionsWithIndexExample extends App {

  // Local Scala collection.
  val days = List("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  // Converting local Scala collection to RDD.
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
  val mappedRDD = daysRDD.mapPartitionsWithIndex((partition, iterator) => {
    val list = iterator.toList
    val tupleList = list.map(element => (element, element.length, partition))
    tupleList.iterator
  })

  // Let's print some data.
  mappedRDD.collect.foreach(println)

  /** Output
   * (Sunday,6,1)
   * (Monday,6,2)
   * (Tuesday,7,3)
   * (Wednesday,9,4)
   * (Thursday,8,5)
   * (Friday,6,6)
   * (Saturday,8,7)
   */
}

package rdd.transformations

import org.apache.spark.sql.SparkSession

object GroupByExample extends App {

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  val wordsRDD = sparkContext.parallelize(Array(
    "Tom", "Lenevo", "Anvisha",
    "John", "Jimmy", "Jacky",
    "John", "Jimmy", "Jimmy"
  ))

  // Group elements based on their length.
  val groupRDD = wordsRDD.groupBy(word => word.length)

  // Let's print some data.
  groupRDD.collect.foreach(println)

  /** Output
   * (3,CompactBuffer(Tom))
   * (4,CompactBuffer(John, John))
   * (5,CompactBuffer(Jimmy, Jacky, Jimmy, Jimmy))
   * (6,CompactBuffer(Lenevo))
   * (7,CompactBuffer(Anvisha))
   */
}

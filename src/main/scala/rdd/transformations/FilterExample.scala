package rdd.transformations

import org.apache.spark.sql.SparkSession

object FilterExample extends App {

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  // RDD containing numbers.
  val numbersRDD = sparkContext.parallelize(1 to 10)

  // Applying filter operation.
  val evenRDD = numbersRDD.filter(number => number % 2 == 0)

  // Printing the RDD
  evenRDD.collect.foreach(println)

}

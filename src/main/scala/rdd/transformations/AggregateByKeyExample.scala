package rdd.transformations

import org.apache.spark.sql.SparkSession

object AggregateByKeyExample extends App {

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext
}


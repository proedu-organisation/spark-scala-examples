package rdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDCreation extends App {

  // Creating a local Scala collection.
  val days = List("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

  // Create SparkConf object.
  val sparkConf = new SparkConf()
  sparkConf.setAppName("RDDCreation")
  sparkConf.setMaster("local[*]")

  // Create SparkContext from SparkConf object.
  val sc = new SparkContext(sparkConf)

  //Using sc (SparkContext) to create RDD.
  val daysRDD = sc.parallelize(days)

  // Collect and show RDD contents.
  daysRDD.collect.foreach(println)

}

package rdd

import org.apache.spark.sql.SparkSession

object EmptyRDDExample extends App {

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  // Create Empty RDD of type String.
  val emptyRDD = sparkContext.emptyRDD[String]
  emptyRDD.saveAsTextFile("src/main/output/EmptyRDD")

  // Let's create RDD of String, but make empty.
  val rdd = sparkContext.parallelize(Seq.empty[String])
  rdd.saveAsTextFile("src/main/output/EmptyRDDNew")

  // Empty Pair RDD.
  type pairRDD = (String,Int)
  val pairRDD = sparkContext.emptyRDD[pairRDD]
  pairRDD.saveAsTextFile("src/main/output/EmptyPairRDD")

}

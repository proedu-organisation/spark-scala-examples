package rdd.transformations

import org.apache.spark.sql.SparkSession

object GroupByKeyExample extends App {

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  val data = Seq(("Apple", 1), ("Banana", 1), ("Apple", 1), ("Apple", 1), ("Orange", 1), ("Orange", 1))

  val dataRDD = sparkContext.parallelize(data)

  // Let's print some data.
  dataRDD.collect.foreach(println)

  /** Output
   * (Apple,1)
   * (Banana,1)
   * (Apple,1)
   * (Apple,1)
   * (Orange,1)
   * (Orange,1)
   */

  // Calling groupByKey transformation in Pair RDD.
  val groupRDD = dataRDD.groupByKey()

  // Let's print some data.
  groupRDD.collect.foreach(println)

  /** Output
   * (Apple,CompactBuffer(1, 1, 1))
   * (Banana,CompactBuffer(1))
   * (Orange,CompactBuffer(1, 1))
   */

  val wordCountRDD = groupRDD.map(tuple => (tuple._1, tuple._2.toList.sum))

  wordCountRDD.collect.foreach(println)

  /**
   * (Apple,3)
   * (Banana,1)
   * (Orange,2)
   */
}


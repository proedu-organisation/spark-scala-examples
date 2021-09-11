package rdd.transformations

import org.apache.spark.sql.SparkSession

object ReduceByKeyExample extends App {

  val file = "src/main/resources/server_log"

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  // Reading Server Log file as RDD.
  val serverLogRDD = sparkContext.textFile(file)

  // Let's print some data.
  serverLogRDD.collect.foreach(println)

  /** Output
   * ERROR	php
   * DONE	php
   * ERROR	RailsApp
   * ERROR	php
   * DONE	mysql
   */

  // Split each line by TAB delimiter.
  val flattenedRDD = serverLogRDD.flatMap(line => line.split("\t"))

  // Let's print some data.
  flattenedRDD.collect.foreach(println)

  /** Output
   * DONE
   * mysql
   * ERROR
   * php
   * ERROR
   * php
   * ERROR
   */

  // Convert each word into a tuple.
  val tupleRDD = flattenedRDD.map(word => (word, 1))

  // Let's print some data.
  tupleRDD.collect.foreach(println)

  /** Output
   * (ERROR,1)
   * (php,1)
   * (DONE,1)
   * (php,1)
   * (ERROR,1)
   */

  // Reducing the tuple to calculate the word count.
  val reducedRDD = tupleRDD.reduceByKey((x, y) => x + y)

  // Let's print some data.
  reducedRDD.collect.foreach(println)

  /** Output
   * (DONE,4)
   * (ERROR,9)
   * (php,7)
   * (RailsApp,3)
   * (mysql,3)
   */

}

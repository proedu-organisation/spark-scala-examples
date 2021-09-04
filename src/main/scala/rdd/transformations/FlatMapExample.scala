package rdd.transformations

import org.apache.spark.sql.SparkSession

object FlatMapExample extends App {

  // Prepare the data.
  val data = Seq(
    "CAT,BAT,RAT,ELEPHANT",
    "RAT,BAT,BAT,BAT,CAT",
    "CAT,ELEPHANT,RAT,ELEPHANT",
    "RAT,RAT,RAT,BAT,CAT"
  )

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  val inputRDD = sparkContext.parallelize(data)
  inputRDD.collect.foreach(println)

  val tupleRDD = inputRDD.map(line => line.split(","))
  tupleRDD.collect.foreach(println)

  /** Output. map does not know how to flatten an Array.
   * [Ljava.lang.String;@2bef09c0
   * [Ljava.lang.String;@62ce72ff
   * [Ljava.lang.String;@58a63629
   * [Ljava.lang.String;@7de843ef
   */

  val tupleRDD1 = inputRDD.flatMap(line => line.split(","))
  tupleRDD1.collect.foreach(println)

  /** Output
   * CAT
   * BAT
   * RAT
   * ELEPHANT
   * RAT
   * BAT
   * BAT
   */

}

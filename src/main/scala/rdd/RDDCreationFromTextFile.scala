package rdd

import org.apache.spark.sql.SparkSession

object RDDCreationFromTextFile extends App {

  val file = "src/main/resources/retail_db/categories"

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  // Set Log level to ERROR
  sparkContext.setLogLevel("ERROR")

  // Reading a text file.
  val dataRDD = sparkContext.textFile(file)

  dataRDD.take(10).foreach(println)
}

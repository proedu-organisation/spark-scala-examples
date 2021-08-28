package rdd

import org.apache.spark.sql.SparkSession

object WholeTextFilesExample extends App {
  val file = "src/main/resources/retail_db/categories-multipart"

  // Creating a SparkContext object.
  val sparkContext = SparkSession.builder()
    .master("local[*]")
    .appName("Proedu.co examples")
    .getOrCreate()
    .sparkContext

  // Set Log level to ERROR
  sparkContext.setLogLevel("ERROR")

  // Reading a text file.
  val dataRDD = sparkContext.wholeTextFiles(file)
  println(dataRDD)

  dataRDD.take(10).foreach(println)
}

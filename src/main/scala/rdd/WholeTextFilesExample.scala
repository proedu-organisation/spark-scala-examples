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

  // Read a directory containing text files.
  val data = sparkContext.textFile(file)
  //data.collect().foreach(println)


  // Reading a text file using wholeTextFiles method.
  val dataRDD = sparkContext.wholeTextFiles(file)
  //dataRDD.take(10).foreach(println)

  // Reading text files with a matching pattern.
  val patternRDD = sparkContext.textFile("src/main/resources/retail_db/categories-multipart/*00001")
  //patternRDD.collect().foreach(println)

  // Reading files from multiple directories
  val multiDirRDD = sparkContext.textFile("src/main/resources/retail_db/categories-multipart/part-m-00001,src/main/resources/retail_db/categories/")
  multiDirRDD.collect().foreach(println)

}

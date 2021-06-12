package cca175

import org.apache.spark.sql.SparkSession

object CCA175ExamScenario1 extends App {
  val spark = SparkSession.builder()
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val orders = spark.read.csv("src/main/resources/retail_db/orders")
  val customers = spark.read.csv("src/main/resources/retail_db/customers")

  orders.show(10,false)
  customers.show(10,false)
}

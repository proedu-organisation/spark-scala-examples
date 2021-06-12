package cca175

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Scenario 15 : CCA 175 Playlist Videos
 */
object CCA175ExamScenario15 extends App {
  val spark = SparkSession.builder()
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val orders = spark.read
    .option("inferSchema", true)
    .csv("src/main/resources/retail_db/orders")
    .toDF("order_id", "order_date", "order_customer_id", "order_status")

  val customers = spark.read
    .option("inferSchema", true)
    .csv("src/main/resources/retail_db/customers")
    .toDF("customer_id", "customer_fname", "customer_lname", "customer_email", "customer_password", "customer_street", "customer_city", "customer_state", "customer_zipcode")

  // Using Spark DSL.
  /*val result = orders.groupBy($"order_customer_id")
    .count()
    .filter($"count" > 5)
    .join(customers, orders.col("order_customer_id") === customers.col("customer_id"))
    .filter(col("customer_fname").like("M%"))
    .select("customer_fname", "customer_lname", "count")
    .sort(desc("count"))*/

  // Using SQL query
  customers.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")

  val query =
              """
              select customer_id, customer_fname, customer_lname, count(1) count
              from customers c join orders o
              on c.customer_id = o.order_customer_id
              where c.customer_fname like 'M%'
              group by customer_id, customer_fname, customer_lname
              having count(1) > 5
              order by count desc
              """

  val result = spark.sql(query)
  result.select("customer_fname", "customer_lname", "count")
    .show(false)

  /*result.repartition(1)
    .map(row => row.mkString("|"))
    .write.option("compression", "gzip")
    .text("src/main/resources/result/scenario15/solution")*/
}

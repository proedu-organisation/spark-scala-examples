package accumulator

import org.apache.spark.sql.SparkSession

object Acc {
  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .master("local[5]")
      .getOrCreate()

    val data = (1 to 1000000)

    var counter = 0
    var orders = spark.sparkContext.parallelize(data)
    orders.foreach(order => counter += order)
    println("Counter Value: " + counter)

  }
}

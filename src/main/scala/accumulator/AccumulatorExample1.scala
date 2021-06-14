package accumulator

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

/**
 * +--------+-------------+-----------------+--------------+
 * |order_id|order_date   |order_customer_id|order_status  |
 * +--------+-------------+-----------------+--------------+
 * |50977   |1402272000000|2856             |COMPLETE      |
 * |10947   |1380499200000|1973             |PAYMENT_REVIEW|
 * |41548   |1396828800000|12225            |PENDING       |
 * |43659   |1398038400000|3830             |COMPLETE      |
 * |43530   |1398038400000|9802             |CLOSED        |
 * +--------+-------------+-----------------+--------------+
 */
object AccumulatorExample1 extends Serializable {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .master("local[5]")
      .getOrCreate()

    // Create LongAccumulator
    val completeAccum: LongAccumulator = spark.sparkContext.longAccumulator("COMPLETED")

    // Reading the parquet file
    val orders = spark.read
      .parquet("src/main/resources/retail_db/orders_parquet")
      .repartition(10)

    // Incrementing the accumulator count in foreach action.
    orders.foreach(
      row => {
        val status = row.getAs[String]("order_status")
        if (status.equals("COMPLETE")) {
          completeAccum.add(1L)
        }
      }
    )

    // Printing the current value of Accumulator. This piece of code runs in Driver.
    println("Accumulator Value: " + completeAccum.value)
  }
}

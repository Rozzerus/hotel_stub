package kafka

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Consumer {

  private final val TOPIC = "RATE_TOPIC"


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("KafkaConsumer")
      .master("local[*]")
      .getOrCreate

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", TOPIC)
      .load()

    df
      .select(
        from_json(
          col("value").cast("string"),
          getRateSchema
        ).as("value").as(RowEncoder(getRateSchema))
      )
      .where(col("month").isNotNull)
      .show(100,1000)
//      .foreach(row => {
//        println(row)
//      })
//      .write
//      .format("console")
//      .save()
//      .start()
//      .format("parquet")
//      .outputMode(OutputMode.Append())
//      .option("checkpointLocation", "F:\\My\\hotel_stub")
//      .start("F:\\My\\hotel_stub")
//      .awaitTermination()
  }

  private def getRateSchema: StructType ={
    StructType(Array(
      StructField("month", IntegerType, nullable = true, Metadata.empty),
      StructField("rate", DoubleType, nullable = true, Metadata.empty)
    ))

  }
}

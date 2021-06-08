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
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", TOPIC)
      .load()

    val result = df
      .select(
        from_json(
          col("value").cast("string"),
          getRateSchema
        ).as("value").as(RowEncoder(getRateSchema))
      )
      .where(col("month").isNotNull)

    val stream = result
      .writeStream
      .queryName("KafkaToCassandraForeach")
      .outputMode("update")
      .foreach(new RateCassandraWriter(spark))
      .start()
    stream.awaitTermination()
  }

  private def getRateSchema: StructType ={
    StructType(Array(
      StructField("month", IntegerType, nullable = true, Metadata.empty),
      StructField("rate", DoubleType, nullable = true, Metadata.empty)
    ))

  }
}

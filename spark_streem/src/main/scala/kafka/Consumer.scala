package kafka

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object Consumer {

  private final val TOPIC = "RATE_TOPIC"


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("KafkaConsumer")
      .master("local[*]")
      .getOrCreate
    readDataAndWriteViaStreaming(spark)

    spark.stop()
  }

  private def readDataAndWriteViaStreaming(spark: SparkSession) = {
    writeStreamingData(applySchema(readStreamingData(spark)))
      .processAllAvailable()
  }

  private def writeStreamingData(data: Dataset[Row]) = {
    data
      .writeStream
      .format("parquet")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "F:\\My\\hotel_stub")
      .start("F:\\My\\hotel_stub")
  }

  private def readStreamingData(spark: SparkSession) = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:9092")
      .option("subscribe", TOPIC)
      .load()
  }

  def applySchema(df: DataFrame) = {
    df
      .select(from_json(col("value").cast("string"), getRateSchema)
        .as("value").as(RowEncoder(getRateSchema)))
  }

  private def getRateSchema: StructType ={
    StructType(Array(
      StructField("dateTime", LongType, nullable = true, Metadata.empty),
      StructField("rate", DoubleType, nullable = true, Metadata.empty)
    ))

  }
}

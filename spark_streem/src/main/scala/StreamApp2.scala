import org.apache.spark._
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object StreamApp2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")


    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    //    18.116.234.239  //A
    //    18.220.249.229  //B
    //    3.142.97.138  //C

    val df: Dataset[Row] = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9080)
      .load()

    import spark.implicits._

    val lines = df.as[String].flatMap(_.split(";"))


    val transformedDf = lines
      .withColumn("_tmp", split(col("value"), ","))
      .select(
        col("_tmp").getItem(0).cast(TimestampType).as("send_time"),
        col("_tmp").getItem(1).as("hotel_id"),
        col("_tmp").getItem(2).as("room_type"),
        col("_tmp").getItem(3).as("price")
      )
//      .groupBy("hotel_id","room_type","price", "send_time")
//      .count()
//      .where(col("hotel_id").isNaN)
      .select("hotel_id","room_type","price", "send_time")
      .withWatermark("send_time", "10 minutes")




    val query = transformedDf.writeStream
      .format("org.apache.spark.sql.cassandra")
      .outputMode("append")
      .option("checkpointLocation", "F:\\My\\hotel_stub")
      .option("table", "price")
      .option("keyspace", "hotel")
      .option("spark.cassandra.connection.host","127.0.0.1")
      .option("spark.cassandra.connection.port","9042")
      .option("spark.cassandra.auth.password", "cassandra")
      .option("spark.cassandra.auth.username", "cassandra")
      .start()

//      .outputMode("complete")
//      .format("console")
//      .start()
    query.awaitTermination()
  }

}

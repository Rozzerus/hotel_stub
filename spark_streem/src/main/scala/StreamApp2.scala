import org.apache.spark._
import org.apache.spark.sql.functions.{col, current_timestamp, split, udf}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.joda.time.DateTime
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.oss.driver.api.core.uuid.Uuids
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamApp2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("NetworkWordCount")
      .set("spark.cassandra.connection.host","127.0.0.1")
      .set("spark.cassandra.connection.port","9042")
      .set("spark.cassandra.auth.password", "cassandra")
      .set("spark.cassandra.auth.username", "cassandra")

//    val ssc = new StreamingContext(conf, Seconds(10))
//    val df = ssc.socketTextStream("localhost", 9080)


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

//    val uuid = udf(() => Uuids.random().toString)

    val transformedDf =
      lines
      .withColumn("_tmp", split(col("value"), ","))
      .select(
        col("_tmp").getItem(0).as("hotel_id"),
        col("_tmp").getItem(1).as("room_type"),
        col("_tmp").getItem(2).as("price")
      )
//      .withColumn("uuid", uuid())
//      .select("uuid","hotel_id","price","room_type")

//
//
//
//
    val query = transformedDf
      .writeStream
      .trigger(Trigger.ProcessingTime("10 second"))
      .foreachBatch({ (batchDf: DataFrame, batchId: Long) =>
        println(s"Writing to cassandra $batchId")
        batchDf
          .write
          .cassandraFormat("price", "hotel")
          .mode("append")
          .save()
      })
      .outputMode("update")
      .start()
//      .format("org.apache.spark.sql.cassandra")
//      .outputMode("append")
//      .option("checkpointLocation", "F:\\My\\hotel_stub")
//      .option("table", "price")
//      .option("keyspace", "hotel")
//      .option("spark.cassandra.connection.host","127.0.0.1")
//      .option("spark.cassandra.connection.port","9042")
//      .option("spark.cassandra.auth.password", "cassandra")
//      .option("spark.cassandra.auth.username", "cassandra")
//      .start()

//
//      .outputMode("append")
//      .format("console")
//      .option("truncate", "false")
//      .start()
    query.awaitTermination()
  }

}

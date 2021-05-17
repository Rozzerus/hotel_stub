import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.streaming._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper

object StreamApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Hotel_stub_reared")
      .set("spark.cassandra.connection.host","127.0.0.1")
      .set("spark.cassandra.connection.port","9042")
      .set("spark.cassandra.auth.password", "cassandra")
      .set("spark.cassandra.auth.username", "cassandra")

    val ssc = new StreamingContext(conf, Seconds(5))

//    18.116.234.239  //A
//    18.220.249.229  //B
//    3.142.97.138  //C
    val stream = ssc
      .socketTextStream("localhost", 9080)
    stream.flatMap(_.split(";")).foreachRDD({ rdd =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      rdd.toDF()
        .withColumn("_tmp", split(col("value"), ","))
        .select(
          col("_tmp").getItem(0).as("hotel_id"),
          col("_tmp").getItem(1).as("room_type"),
          col("_tmp").getItem(2).as("price")
        )
        .write
        .cassandraFormat("price", "hotel")
        .mode("append")
        .save()
    })

    ssc.start()
    ssc.awaitTermination()
  }

}

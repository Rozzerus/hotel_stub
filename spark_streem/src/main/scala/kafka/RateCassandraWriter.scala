package kafka

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

//https://blog.knoldus.com/creating-data-pipeline-with-spark-streaming-kafka-and-cassandra/
class RateCassandraWriter (spark: SparkSession) extends ForeachWriter[Row] {

  val keyspace = "hotel"
  val table = "rate"
  val connector: CassandraConnector = CassandraConnector(spark.sparkContext.getConf)

  override def open(partitionId: Long, epochId: Long): Boolean = {
    println("Open connection.")
    true
  }

  override def process(row: Row): Unit = {

    val month = row.getAs[Int]("month").toString
    val rate = row.getAs[Double]("rate").toString
    connector.withSessionDo { session =>
      session.execute(
        s"""
           |insert into $keyspace.$table("month", "rate")
           |values (${month}, ${rate})
           """.stripMargin)
    }
  }

  override def close(errorOrNull: Throwable): Unit = println("Closing connection.")

}
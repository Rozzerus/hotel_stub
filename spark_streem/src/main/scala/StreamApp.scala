import org.apache.spark._
import org.apache.spark.streaming._

object StreamApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

//    18.116.234.239  //A
//    18.220.249.229  //B
//    3.142.97.138  //C
    val df = ssc
      .socketTextStream("3.142.97.138", 9080)
    val words = df.flatMap(_.split(";"))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

package kafka

import kafka.message.DefaultCompressionCodec
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS
import java.util.Properties
import scala.util.Random

object Producer {

  private final val TOPIC = "RATE_TOPIC"

  private val summerMounts = List(6,7,8)
  private val greatMounts = List(5,9,12)

  def main(args: Array[String]): Unit = {
    val countMessages = 40
    val producer = createProducer()
    for (x <- 1 to countMessages) {
      println("n: %s, thread: %s".format(x, Thread.currentThread.getId))
      sendMessageToKafka(createMessage(), producer, TOPIC).get()
      Thread.sleep(1000)
    }

  }

   def sendMessageToKafka(message: Rate, producer: Producer[Any, Rate], topic: String) = {
     println("Send message: " + message)
     val record: ProducerRecord[Any, Rate] = new ProducerRecord(topic, message)
     producer.send(record)
   }

  def createProducer(): Producer[Any, Rate] = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, DefaultCompressionCodec.name)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "console-producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "kafka.RateSerializer")
    new KafkaProducer[Any, Rate](props)
  }

  def randomDate(from: LocalDate, to: LocalDate): LocalDate = {
    val diff = DAYS.between(from, to)
    val random = new Random(System.nanoTime)
    from.plusDays(random.nextInt(diff.toInt))
  }

  def rate(date: LocalDate): Double = {
    val r: scala.util.Random = new Random
    var iCoefficient = 100
    if (summerMounts.contains(date.getMonth.getValue)){
      iCoefficient = 150
    }
    if (greatMounts.contains(date.getMonth.getValue)){
      iCoefficient = 120
    }
    Math.floor((1 + r.nextDouble()) * iCoefficient) / 100
  }

  def createMessage(): Rate = {
    val fromDate = LocalDate.of(2021, 1, 1)
    val toDate = LocalDate.of(2021, 12, 31)
    val randomDateForMessage = randomDate(fromDate, toDate)

    new Rate(
      randomDateForMessage.getMonthValue,
      rate(randomDateForMessage)
    )
  }


}

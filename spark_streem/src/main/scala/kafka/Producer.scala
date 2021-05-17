package kafka

import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

object Producer {

  private final val TOPIC = "RATE_TOPIC"

  def main(args: Array[String]): Unit = {
    var countMessages = 40
    var countThreads = 4
    if (args.length == 2 && args.apply(0) != null) {
      countMessages = args.apply(0).toInt
      countThreads = args.apply(1).toInt
    } else {
      println("Params are wrong. Using default params. Default params are countMessages = 40, countThreads = 4")
    }
    val producer = ProducerFactory.createProducer()
    for (x <- 1 to countMessages / countThreads) {
      println("n: %s, thread: %s".format(x, Thread.currentThread.getId))
      sendMessageInKafka(RateFactory.createMessage(), producer, TOPIC).get()
    }

  }

   /** send message in broker topic
     * @param message message to be sent
     * @return producer
     */
   def sendMessageInKafka(message: Rate, producer: Producer[Any, Rate], topic: String) = {
     println("Send message: " + message)
     val record: ProducerRecord[Any, Rate] = new ProducerRecord(topic, message)
     producer.send(record)
   }
}

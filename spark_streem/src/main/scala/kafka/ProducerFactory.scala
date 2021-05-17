package kafka

import java.util.Properties

import kafka.message.DefaultCompressionCodec
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}

object ProducerFactory {



  def createProducer(): Producer[Any, Rate] = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, DefaultCompressionCodec.name)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "console-producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "kafka.RateSerializer")
    new KafkaProducer[Any, Rate](props)
  }

}

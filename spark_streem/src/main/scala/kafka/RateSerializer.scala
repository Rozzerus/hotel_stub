package kafka

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.Serializer

class RateSerializer extends  Serializer[Rate]{


  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    // nothing to do
  }

  override def serialize(topic: String, train: Rate): Array[Byte] = {
    val objectMapper = new ObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.writeValueAsString(train).getBytes
  }

  override def close (): Unit = {
    // nothing to do
  }

}

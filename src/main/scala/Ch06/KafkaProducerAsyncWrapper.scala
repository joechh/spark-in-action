package Ch06

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer

case class KafkaProducerWrapper(brokerList: String) {
  val producerProps = {
    val prop = new java.util.Properties()
    prop.put("bootstrap.servers", brokerList)
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("producer.type", "async")

    prop
  }
  val producer = new KafkaProducer[String, String](producerProps)

  def send(topic: String, key: String = null, value: String): Unit = producer.send(new ProducerRecord(topic, key, value))

}

object KafkaProducerWrapper {
  var brokerList = ""
  lazy val instance = new KafkaProducerWrapper(brokerList)
}




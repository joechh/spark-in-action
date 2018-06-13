package Ch06

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaProducerHelper(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, key: String = null, value: String): Unit = producer.send(new ProducerRecord(topic, key, value))
}

object KafkaProducerHelper {

  def apply(config: Properties): KafkaProducerHelper = {
    val f = () => {
      new KafkaProducer[String, String](config)
    }
    new KafkaProducerHelper(f)
  }
}

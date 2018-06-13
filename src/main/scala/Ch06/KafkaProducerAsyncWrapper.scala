package Ch06

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord}

case class KafkaProducerAsyncWrapper(brokerList: String) {
  val producerProps = {
    val prop = new java.util.Properties()
    prop.put("bootstrap.servers", brokerList)
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("producer.type", "async")

    prop
  }
  val producer = new KafkaProducer[String, String](producerProps)

  def send(topic: String, key: String = null, value: String): Unit = {
    val record = new ProducerRecord(topic, key, value)
    producer.send(record, new ProducerCallback())
  }

  import org.apache.kafka.clients.producer.RecordMetadata

  private class ProducerCallback extends Callback with Serializable {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      if (e != null) e.printStackTrace()
    }
  }

}






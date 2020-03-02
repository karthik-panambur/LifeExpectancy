  import java.util.Properties
  import org.apache.kafka.clients.producer._
  import org.slf4j.impl.StaticLoggerBinder
  object Producer {
    def main(args: Array[String]): Unit = {
      writeToKafka("Scalatest")
    }
    def writeToKafka(topic: String): Unit = {
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](props)
      val record = new ProducerRecord[String, String](topic, "Hello", "Karthik")
      producer.send(record)
      producer.close()
    }
  }


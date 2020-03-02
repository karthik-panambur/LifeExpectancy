package com.bigdata.kafka

package com.kafka.projects

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.io._
import java.util
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer

object Consumer {

  def main(args: Array[String]): Unit = {
    /*val topicName = "Streaming"
    println("Creating Kafka Producer...")
    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "Streaming")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topicName))
    while ( {
      true
    }) {
      val consumerRecords = consumer.poll(50000)
      import scala.collection.JavaConversions._
      for (record <- consumerRecords) {
        System.out.println(record.value.toString)
      }
    }*/

    val props = new Properties
    //val producer =
    //props.put("auto.offset.reset", "earliest")
    //props.put("enable.auto.commit", "false")
    props.put("enable.auto.commit","false")
    props.put("auto.offset.reset","earliest")
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "myGrp")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(java.util.Collections.singletonList("firsttopic"))
    while (true) {
      val records = consumer.poll(5000)
      for (record <- records.asScala)
        println(record.value())
    }
  }
}
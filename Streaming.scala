//package com.bigdata.kafka
//
//package com.kafka.projects
//
//import org.apache.kafka.common.serialization.Serdes
//import org.apache.kafka.streams.KafkaStreams
//import org.apache.kafka.streams.StreamsBuilder
//import org.apache.kafka.streams.StreamsConfig
//import org.apache.kafka.streams.Topology
//import org.apache.kafka.streams.kstream.KStream
//import org.apache.spark.ml.ann.Topology
//import java.util.Properties
//
//
///**
// * Kafka Streams application that reads and prints the messages from a given topic
// *
// *
// */
//object Streaming {
//  private val topicName = "Streaming"
//
//  /**
//   * Application entry point
//   *
//   * @param args topicName (Name of the Kafka topic to read)
//   */
//  def main(args: Array[String]): Unit = {
//    val props = new Properties
//    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Streaming")
//    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
//    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
//    val builder = new Nothing
//    val kStream = builder.stream(topicName)
//    kStream.foreach((k, v) => System.out.println(" Value = " + v))
//    //kStream.peek((k, v) -> System.out.println("Key = " + k + " Value = " + v));
//    val topology = builder.build
//    val streams = new Nothing(topology, props)
//    println("Starting the stream")
//    streams.start
//    Runtime.getRuntime.addShutdownHook(new Thread(() => {
//      def foo() = {
//        println("Stopping Stream")
//        streams.close
//      }
//
//      foo()
//    }))
//  }
//}


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

//object KafkaDataStream{

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <groupId> is a consumer group name to consume from topics
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    consumer-group topic1,topic2
 */
object KafkaDataStream {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <groupId> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <groupId> is a consumer group name to consume from topics
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    //  StreamingExamples.setStreamingLogLevels()

    val Array(brokers, groupId, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
//}
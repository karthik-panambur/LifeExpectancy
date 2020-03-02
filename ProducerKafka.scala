package com.bigdata.kafka

package com.kafka.projects

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.serialization.StringSerializer
import java.io._
import java.util
import java.util.Properties
import scala.io

/**
 * A Kafka producer that sends numEvents (# of messages) to a given topicName
 *
 * @author prashant
 * @author www.learningjournal.guru
 */
object Producer {

  def main(args: Array[String]): Unit = {

    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](props)
  var count =0;
    var lineCount =0;
    while (count < 5) {
      val bufferedSource = io.Source.fromFile("/Users/mahesh/Desktop/PBDA/Project/Life Expectancy Data_Copy.csv")
      for (line <- bufferedSource.getLines) {
        lineCount = lineCount +1;
        val cols = line.split("\n").map(_.trim)
        for (record <- cols) {
          //println("record" + record)
          var timestamp: Long = System.currentTimeMillis / 1000
          //println(timestamp + ""+lineCount +" r " + record;
          var recordModified = "";
          if (lineCount == 1){
            recordModified = "id"  +"," + record
          }
          else {
            recordModified = timestamp + "" + lineCount + "" + count+ "," + record
          }

          val information = new ProducerRecord[String, String]("testing", "key", recordModified)
          println(recordModified)
          producer.send(information)

        }
      }
      count=count+1;
      bufferedSource.close
    }

    println("send")
    producer.close()
  }
}

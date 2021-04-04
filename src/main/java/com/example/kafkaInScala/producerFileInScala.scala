package com.example.kafkaInScala

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import java.io.FileInputStream
import java.util.Properties
import scala.io.Source

object producerFileInScala {
  def main(args: Array[String]): Unit = {

    val topicName= "kafka-spark"
    val properties = new Properties()
    properties.load(new FileInputStream("C://Users//roshan_singh//IdeaProjects//Java_kafka_project//src//main//resources//producer.properties"))

    val producer = new KafkaProducer[String,String](properties)
    val fname= "C://Users//roshan_singh//IdeaProjects//Java_kafka_project//Data//readingtxt.txt"
    val fsource= Source.fromFile(fname)
    producer.send(new ProducerRecord[String,String](topicName,"101",fname))

    for( lines <- fsource.getLines()){

      producer.send(new ProducerRecord[String,String](topicName,lines))

    }
    fsource.close()
    producer.flush()
  }


}

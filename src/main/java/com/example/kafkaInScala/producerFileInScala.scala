package com.example.kafkaInScala

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import java.util.Properties
import scala.io.Source

object producerFileInScala {
  def main(args: Array[String]): Unit = {

    val topicName= "kafka-spark"

    val producerProperties=new Properties()

    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[IntegerSerializer].getCanonicalName)
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getCanonicalName)

    val producer = new KafkaProducer[Int,String](producerProperties)
    val fname= "C://Users//roshan_singh//IdeaProjects//Java_kafka_project//Data//readingtxt.txt"
    val fsource= Source.fromFile(fname)
    producer.send(new ProducerRecord[Int,String](topicName,101,fname))

    for( lines <- fsource.getLines()){

      producer.send(new ProducerRecord[Int,String](topicName,10,lines))

    }
    fsource.close()
    producer.flush()
  }


}

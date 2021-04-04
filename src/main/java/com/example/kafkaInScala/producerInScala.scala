package com.example.kafkaInScala

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import java.io.FileInputStream
import java.util.Properties

object producerInScala extends  App {

  val topicName= "kafka-spark"

  val prop = new Properties()
  prop.load(new FileInputStream("C://Users//roshan_singh//IdeaProjects//Java_kafka_project//src//main//resources//producer.properties"))

  val producer = new KafkaProducer[String,String](prop)

  producer.send(new ProducerRecord[String,String](topicName,"10", "Scala Producer"))

  producer.flush()




}

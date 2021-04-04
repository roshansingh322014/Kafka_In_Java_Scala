package com.example.kafkaInScala

import org.apache.kafka.clients.consumer.ConsumerConfig.{AUTO_OFFSET_RESET_CONFIG, BOOTSTRAP_SERVERS_CONFIG, ENABLE_AUTO_COMMIT_CONFIG, GROUP_ID_CONFIG, KEY_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS_CONFIG}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import org.apache.log4j.Logger

import java.io.FileInputStream
import scala.collection.JavaConverters._
import java.util.Properties

object consumerInScala extends App {
  val logger = Logger.getLogger(this.getClass.getName);
  val topicName = "kafka-spark"

  val properties = new Properties()
  properties.load(new FileInputStream("C://Users//roshan_singh//IdeaProjects//Java_kafka_project//src//main//resources//consumer.properties"))


  val consumer = new KafkaConsumer[String, String](properties) // setting up the consumer for the kafka

  consumer.subscribe(List(topicName).asJava) // consumer subscribing the kafka topics

  println(" Key | Value | Partition | Offset ") // to generate the header of the show records
  while (true) {
    val polledrecords: ConsumerRecords[String, String] = consumer.poll(1)


    if (!polledrecords.isEmpty) {
      println(s"Polled ${polledrecords.count()} records")
      val recordIterator = polledrecords.iterator()
      while (recordIterator.hasNext) {
        val record = recordIterator.next();
        println(s" ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()}")
        logger.info("successfully passed a message ...: " + record.value())
      }
    }
  }
}

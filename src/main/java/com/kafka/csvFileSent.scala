package com.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.io.FileInputStream
import java.util.Properties
import scala.io.Source

object csvFileSent extends  App {

  val topicName= "kafka-spark"
  val properties = new Properties()
  properties.load(new FileInputStream("C://Users//roshan_singh//IdeaProjects//Java_kafka_project//src//main//resources//producer.properties"))

  val producer = new KafkaProducer[String,String](properties)
  val fname= "C://Users//roshan_singh//IdeaProjects//Java_kafka_project//Data//demoname.csv"
  val fsource= Source.fromFile(fname)
  producer.send(new ProducerRecord[String,String](topicName,"101",fname))

  for( lines <- fsource.getLines()){

    producer.send(new ProducerRecord[String,String](topicName,lines))

  }
  fsource.close()
  producer.flush()



}

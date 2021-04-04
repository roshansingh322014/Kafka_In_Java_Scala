package com.example.kafkaInJava;

//import com.kafka.example.Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.*;


public class producerInJava {


    public static void main(String[] args) throws IOException {
        String topic = "kafka-spark";

        final Logger logger = LoggerFactory.getLogger(Producer.class);

        FileReader reader = new FileReader("C://Users//roshan_singh//IdeaProjects//Java_kafka_project//src//main//resources//producer.properties");

        // create properties object
        Properties p = new Properties();

        // Add a wrapper around reader object
        p.load(reader);

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(p);

        //create the Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Producer in Java");
        // same keys goes to same partition
        //send data in asynchronous mode
        producer.send(record);

        //flush data
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}

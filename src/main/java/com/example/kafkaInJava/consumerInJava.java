package com.example.kafkaInJava;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class consumerInJava {

    public static void main(String[] args) throws IOException {




        String topic="kafka-spark";

        FileReader reader = new FileReader("C://Users//roshan_singh//IdeaProjects//Java_kafka_project//src//main//resources//consumer.properties");

        // create properties object
        Properties properties = new Properties();

        // Add a wrapper around reader object
        properties.load(reader);
        // create Consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(properties);

        // subscripe consumer to topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while(true){

            ConsumerRecords<String, String> record=consumer.poll(10);
            for(ConsumerRecord<String,String> records : record){
                System.out.println(records.value());
            }


        }





        //earliest means that we want to read from the begining and latest means we want to read new
        //messages
    }
}

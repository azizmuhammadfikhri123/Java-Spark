package com.example.spark_streaming.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.Properties;

public class ProducerExample {
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092"); // Sesuaikan dengan broker Kafka Anda
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(properties);

    String topic = "transjakarta-topic";
    String value = "bissmillah";
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
    producer.send(record);
  }
}

package com.example.spark_streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Properties;

public class SparkStreaming {
  public static void main(String[] args) throws InterruptedException {
    SparkConf conf = new SparkConf()
      .setAppName("SparkStreamingWithKafka")
      .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

    JavaDStream<String> dataTransjakarta = ssc.textFileStream("/home/be-azizmuhammadf/Documents/transjakarta/");

    dataTransjakarta.foreachRDD(rdd -> {
      rdd.foreach(value -> {
        producerKafka(value);
      });
    });

    ssc.start();
    ssc.awaitTermination();
  }

  public static void producerKafka(String value){
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(properties);
    String topic = "transjakarta-topic";

    ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
    producer.send(record, (metadata, exception) -> {
      if (exception == null) {
        System.out.println("Send successfully: " + metadata);
      } else {
        exception.printStackTrace();
      }
    });
  }
}

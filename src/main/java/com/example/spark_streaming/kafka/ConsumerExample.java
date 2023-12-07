package com.example.spark_streaming.kafka;

import com.example.model.TransJakarta;
import com.example.util.JerseyRequest;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerExample {
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-spark");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    properties.setProperty("auto.commit.interval.ms", "1000");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    String topic = "transjakarta-topic";
    Consumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Arrays.asList(topic));

    while (true){
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      records.forEach(record -> {
        String[] recordFields = record.value().split(";");

        String geoLocation = recordFields[11];
        String cleanedGeoLocation = geoLocation.replaceAll("[\"\\[\\]]", "");
        String[] coordinate = cleanedGeoLocation.split(",");

        String hasheId = DigestUtils.md5Hex(recordFields[0] + recordFields[1] + recordFields[2] + recordFields[3] + recordFields[4] + recordFields[5] + recordFields[6] + recordFields[7] + recordFields[8] + recordFields[9] + recordFields[10] + recordFields[11]);
        TransJakarta transJakarta = new TransJakarta(
          hasheId,
          recordFields[0].equals("null") ? null : Integer.valueOf(recordFields[0]),
          recordFields[1].equals("null") ? null : recordFields[1],
          recordFields[2].equals("null") ? null : recordFields[2],
          recordFields[3].equals("null") ? null : recordFields[3],
          recordFields[4].equals("null") ? null : Double.valueOf(recordFields[4]),
          recordFields[5].equals("null") ? null : recordFields[5],
          recordFields[6].equals("null") ? null : Integer.valueOf(recordFields[6]),
          recordFields[7].equals("null") ? null : recordFields[7],
          recordFields[8].equals("null") ? null : recordFields[8],
          recordFields[9].equals("null") ? null : Double.valueOf(recordFields[9]),
          recordFields[10].equals("null") ? null : recordFields[10],
          (coordinate.length > 1 && coordinate[1] != null && !coordinate[1].isEmpty()) ? Double.valueOf(coordinate[1]) : null,
          (coordinate.length > 0 && coordinate[0] != null && !coordinate[0].isEmpty()) ? Double.valueOf(coordinate[0]) : null
        );

        try {
          Map<String, Object> transjakartaMap = new LinkedHashMap<>();
          transjakartaMap.put("id", transJakarta.getId());
          transjakartaMap.put("koridor", transJakarta.getKoridor());
          transjakartaMap.put("current_trip_id", transJakarta.getCurrent_trip_id());
          transjakartaMap.put("color", transJakarta.getColor());
          transjakartaMap.put("gps_datetime", transJakarta.getGps_datetime());
          transjakartaMap.put("dtd", transJakarta.getDtd());
          transjakartaMap.put("trip_name", transJakarta.getTrip_name());
          transjakartaMap.put("course", transJakarta.getCourse());
          transjakartaMap.put("location", transJakarta.getLocation());
          transjakartaMap.put("bus_code", transJakarta.getBus_code());
          transjakartaMap.put("speed", transJakarta.getSpeed());
          transjakartaMap.put("timestamp", transJakarta.getTimestamp());
          transjakartaMap.put("latitude", transJakarta.getLatitude());
          transjakartaMap.put("longitude", transJakarta.getLongitude());

          JerseyRequest jerseyRequest = new JerseyRequest();
          JsonNode create = jerseyRequest.create("http://192.168.20.90:9200/transjakarta/_doc/" + hasheId, transjakartaMap);
          JsonNode findDataTransjakarta = jerseyRequest.findById("http://192.168.20.90:9200/transjakarta/_doc/" + hasheId);
          System.out.println(findDataTransjakarta.get("_source"));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }
}

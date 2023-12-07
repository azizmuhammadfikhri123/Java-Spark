package com.example.csv;

import com.amazonaws.thirdparty.jackson.databind.ObjectMapper;
import com.example.model.TransJakarta;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CsvSaveToElastic {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("CsvSaveToElastic").set("es.index.auto.create", "true")
      .set("spark.driver.allowMultipleContexts", "true").set("es.nodes", "192.168.20.90:9200")
      .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    try {
      JavaRDD<String> csvData = sc.textFile("/home/be-azizmuhammadf/Documents/transjakarta/combine.csv");
      String header = csvData.first();
      JavaRDD<String> dataWithoutHeader = csvData.filter(row -> !row.equals(header));

      JavaRDD<String> dataRdd = dataWithoutHeader.flatMap(value -> {
        ObjectMapper mapper = new ObjectMapper();
        List<String> jsonList = new ArrayList<>();
        TransJakarta transJakarta = null;

        try {
          String[] data = value.split(";");

          String geoLocation = data[11];
          String cleanedGeoLocation = geoLocation.replaceAll("[\"\\[\\]]", "");
          String[] dataArray = cleanedGeoLocation.split(",");

          String id = DigestUtils.md5Hex(data[3] + data[10]);
          transJakarta = new TransJakarta(id, Integer.valueOf(data[0]), data[1], data[2], data[3], Double.valueOf(data[4]), data[5], Integer.valueOf(data[6]), data[7], data[8], Double.valueOf(data[9]), data[10], Double.valueOf(dataArray[0]), Double.valueOf(dataArray[1]));

          String json = mapper.writeValueAsString(transJakarta);
          jsonList.add(json);
        } catch (Exception e) {
          // Log the exception and continue with the next element
          System.err.println("Error processing row: " + value);
          e.printStackTrace();
        }

        return jsonList.iterator();
      });

       JavaEsSpark.saveJsonToEs(dataRdd, "learning_spark/_doc", ImmutableMap.of("es.mapping.id", "id"));
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      sc.stop();
    }
  }
}

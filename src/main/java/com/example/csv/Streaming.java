package com.example.csv;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.*;
import java.util.concurrent.TimeoutException;


public class Streaming {
  public static void main(String[] args) throws InterruptedException, StreamingQueryException, TimeoutException {
    // Inisialisasi SparkConf
    SparkConf conf = new SparkConf()
      .setAppName("SparkStreamingExample")
      .setMaster("local[*]");

    // Inisialisasi SparkSession
    SparkSession spark = SparkSession.builder()
      .config(conf)
      .getOrCreate();

    // Inisialisasi JavaStreamingContext
    JavaStreamingContext jssc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), Durations.seconds(1));

    // Baca data streaming dari CSV
    JavaDStream<String> csvData = jssc.textFileStream("/home/be-azizmuhammadf/Documents/transjakarta/");

    // Ambil header
    csvData.foreachRDD(rdd -> {
      if (!rdd.isEmpty()) {
        String header = rdd.first();

        // Hapus header dari RDD
        JavaRDD<String> dataWithoutHeader = rdd.filter(row -> !row.equals(header));

        JavaRDD<Row> transformedData = dataWithoutHeader.flatMap(row -> {
          List<String> values = Arrays.asList(row.split(";"));

          if (values.size() >= 12) {
            try {
              int koridor = Integer.parseInt(values.get(0));
              String current_trip_id = values.get(1);
              String color = values.get(2);
              String gps_datetime = values.get(3);
              double dtd = Double.parseDouble(values.get(4));
              String trip_name = values.get(5);
              int course = Integer.parseInt(values.get(6));
              String location = values.get(7);
              String bus_code = values.get(8);
              double speed = Double.parseDouble(values.get(9));
              String timestamp = values.get(10);

              String dataString = values.get(11);
              String cleanedDataString = dataString.replaceAll("[\"\\[\\]]", "");
              String[] dataArray = cleanedDataString.split(",");

              if (dataArray.length >= 2) {
                double latitude = Double.parseDouble(dataArray[1]);
                double longitude = Double.parseDouble(dataArray[0]);

                List<Object> correctedValues = Arrays.asList(
                  koridor, current_trip_id, color, gps_datetime, dtd, trip_name,
                  course, location, bus_code, speed, timestamp, latitude, longitude
                );

                return Collections.singletonList(RowFactory.create(correctedValues.toArray())).iterator();
              }
            } catch (NumberFormatException e) {
              e.printStackTrace();
            }
          }

          return Collections.emptyIterator();
        });


        // Definisikan skema kolom
        StructType schema = new StructType()
          .add("koridor", DataTypes.IntegerType, false)
          .add("current_trip_id", DataTypes.StringType, false)
          .add("color", DataTypes.StringType, false)
          .add("gps_datetime", DataTypes.StringType, false)
          .add("dtd", DataTypes.DoubleType, false)
          .add("trip_name", DataTypes.StringType, false)
          .add("course", DataTypes.IntegerType, false)
          .add("location", DataTypes.StringType, false)
          .add("bus_code", DataTypes.StringType, false)
          .add("speed", DataTypes.DoubleType, false)
          .add("timestamp", DataTypes.StringType, false)
          .add("latitude", DataTypes.DoubleType, false)
          .add("longitude", DataTypes.DoubleType, false);

        // Buat DataFrame
        Dataset<Row> transformedDataFrame = spark.createDataFrame(transformedData, schema);

        // Tampilkan hasil transformasi
        transformedDataFrame.show( 10, false);
//        transformedDataFrame.printSchema();
        System.out.println("total " + transformedDataFrame.count());
      }
    });

    // Jalankan streaming
    jssc.start();
    jssc.awaitTermination();
  }
}

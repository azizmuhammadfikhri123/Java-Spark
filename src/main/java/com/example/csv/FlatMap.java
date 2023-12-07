package com.example.csv;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FlatMap {
  public static void main(String[] args) throws InterruptedException {
    // Inisialisasi SparkSession
    SparkSession spark = SparkSession.builder()
      .appName("FlatMapExample")
      .master("local[*]")
      .getOrCreate();

    // Inisialisasi JavaSparkContext
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

    // Baca file CSV sebagai RDD
    JavaRDD<String> csvData = jsc.textFile("/home/be-azizmuhammadf/Documents/transjakarta/combine.csv");

    // Ambil header
    String header = csvData.first();

    // ambil data tanpa header
    JavaRDD<String> dataWithoutHeader = csvData.filter(row -> !row.equals(header));
//    System.out.println("data aziz 1 = " +  dataWithoutHeader.take(2));
//    System.out.println("data aziz 2 = " + dataWithoutHeader.take(2).get(0));

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
    transformedDataFrame.show(10, false);
    // System.out.println("data" + transformedDataFrame.select("location").collectAsList().get(2).getString(0));
    transformedDataFrame.printSchema();

//      for (Row a : transformedDataFrame.select("latitude").collectAsList()){
//        if (a.length() > 0){
//          System.out.println(a.getDouble(0));
//        }
//      }

//    String filePathOutput = "../JavaSpark/src/main/resources/result_transjakarta";
//    transformedDataFrame
//      .coalesce(1)  // Menggabungkan semua partisi menjadi satu
//      .write()
//      .option("header", "true")  // Menyertakan header
//      .csv(filePathOutput);

    jsc.stop();
    spark.stop();
  }
}




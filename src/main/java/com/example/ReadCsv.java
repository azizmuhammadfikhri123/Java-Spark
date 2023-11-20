package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadCsv {
  public static void main(String[] args) {
    // Inisialisasi sesi Spark
    SparkSession spark = SparkSession.builder()
      .appName("ReadCSVSpark")
      .master("local[*]")  // Sesuaikan dengan mode cluster Anda jika diperlukan
      .getOrCreate();

    // Path file CSV
    String csvFilePath = "../JavaSpark/src/main/resources/sample_csv.csv";

    // Membaca file CSV ke DataFrame
    Dataset<Row> df = spark.read()
      .option("header", "true") // Jika file CSV memiliki header
      .csv(csvFilePath);

    // Menampilkan schema DataFrame
    System.out.println("DataFrame");
    df.printSchema();

    // Menampilkan isi DataFrame
    df.show(false);
    System.out.println("value dataframe");

    // Menampilkan isi DataFrame group 1
    df.select("Group 1").show(false);
    System.out.println("value dataframe group 1");

    // Menutup sesi Spark
    spark.stop();
  }
}

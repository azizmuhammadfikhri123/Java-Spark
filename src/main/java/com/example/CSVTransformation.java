package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVTransformation {
  public static void main(String[] args) {
    SparkSession sparkSession = SparkSession.builder()
      .appName("CSVTransformation")
      .master("local[*]")
      .getOrCreate();

    String filePath = "../JavaSpark/src/main/resources/dma.csv";

    Dataset<Row> dataset = sparkSession.read()
      .option("header", true)
      .csv(filePath);

    System.out.println("schema dataframe sebelum di ubah");
    dataset.printSchema();

    System.out.println("data sebelum transformasi");
    dataset.show(false);

    // Transformasi data: Menambahkan kolom baru "isCityCapital" dengan nilai true jika region mengandung "City", false jika tidak
    dataset = dataset.withColumn("isCityCapital", dataset.col("region").contains("City"));

    // Transformasi data: Mengubah type data
    dataset = dataset.withColumn("dma code", dataset.col("dma code").cast("integer"));

    dataset.printSchema();
    System.out.println("schema setelah diubah");

    System.out.println("data setelah transformasi");
    dataset.show(false);

    String filePathOutput = "../JavaSpark/src/main/resources/result_dma";
    dataset.write().csv(filePathOutput);

    sparkSession.stop();
  }
}

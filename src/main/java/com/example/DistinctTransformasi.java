package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class DistinctTransformasi {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext("local", "Distinct Transformation Example");

    JavaRDD<Integer> data = sc.parallelize(Arrays.asList(1, 2, 3, 1, 2, 4));
    JavaRDD<Integer> distinctData = data.distinct();

    System.out.println("Original Data: " + data.collect());
    System.out.println("Distinct Data: " + distinctData.collect());

//    =========================================
//    Original Data: [1, 2, 3, 1, 2, 4]
//    Distinct Data: [1, 2, 3, 4]
    sc.stop();
  }
}

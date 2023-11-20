package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SortTransformasi {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext("local", "Sort Transformation Example");

    JavaRDD<Integer> data = sc.parallelize(Arrays.asList(4, 2, 1, 5, 3));
    JavaRDD<Integer> sortedData = data.sortBy(x -> x, true, 1);

    System.out.println("Original Data: " + data.collect());
    System.out.println("Sorted Data: " + sortedData.collect());
// =========================================================
//    Original Data: [4, 2, 1, 5, 3]
//    Sorted Data: [1, 2, 3, 4, 5]
  }
}

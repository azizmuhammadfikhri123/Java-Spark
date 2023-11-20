package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class ReduceTransformasi {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext("local", "Reduce Transformation Example");

    JavaRDD<Integer> data = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    int sum = data.reduce((x, y) -> x + y);

    System.out.println("Original Data: " + data.collect());
    System.out.println("Sum: " + sum);
// ==================================================
//    Original Data: [1, 2, 3, 4, 5]
//    Sum: 15

    sc.stop();
  }
}

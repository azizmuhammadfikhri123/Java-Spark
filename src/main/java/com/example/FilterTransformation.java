package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class FilterTransformation {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext("local", "Filter Transformation Example");

    List<Integer> values = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> data = sc.parallelize(values);
    JavaRDD<Integer> filterData = data.filter(x -> x % 2 == 0);

    System.out.println("Original data: " + data.collect());
    System.out.println("Filter Data: " + filterData.collect());

//    ===============================
//    Original Data: [1, 2, 3, 4, 5]
//    Filter Data: [2, 4]

    sc.stop();
  }
}

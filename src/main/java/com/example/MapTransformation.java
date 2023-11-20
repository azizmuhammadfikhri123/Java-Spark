package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class MapTransformation {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext("local", "Map Transformation Example");

    List<Integer> values = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> data = sc.parallelize(values);
    JavaRDD<Integer> squareData = data.map(x -> x * x);

    System.out.println("Original Data : " + data.collect());
    System.out.println("Square Data : " + squareData.collect());

//  ======================================
//  Original Data: [1, 2, 3, 4, 5]
//  Squared Data: [1, 4, 9, 16, 25]

    sc.stop();
  }
}

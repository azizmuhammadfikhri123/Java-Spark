package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class FlatMapTransformation {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext("local",  "FlatMap Transformation Example");

    List<String> values = Arrays.asList("Hello World", "Spark Transformation");
    JavaRDD<String> lines = sc.parallelize(values);
    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

    System.out.println("Original Lines: " + lines.collect());
    System.out.println("Words: " + words.collect());

//  ===========================================================
//    Original Lines: [Hello World, Spark Transformation]
//    Words: [Hello, World, Spark, Transformation]

    sc.stop();
  }
}

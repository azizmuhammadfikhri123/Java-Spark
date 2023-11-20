package com.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class MapToPair {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext("local", "MapToPair Transformation Example");

    JavaRDD<String> data = sc.parallelize(Arrays.asList("apple", "banana", "orange"));
    JavaPairRDD<String, Integer> pairRDD = data.mapToPair(word -> new Tuple2<>(word, word.length()));

    System.out.println("Original Data: " + data.collect());
    System.out.println("Mapped to Pair: " + pairRDD.collect());
//=================================================
//    Original Data: [apple, banana, orange]
//    Mapped to Pair: [(apple,5), (banana,6), (orange,6)]

    sc.stop();
  }
}

package com.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class GroupByKeyTransformasi {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext("local", "GroupBy Transformation Example");

    JavaPairRDD<String, Integer> data = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("A", 1),
      new Tuple2<>("B", 2),
      new Tuple2<>("A", 3),
      new Tuple2<>("B", 4)
    ));

    JavaPairRDD<String, Iterable<Integer>> groupedData = data.groupByKey();

    System.out.println("Original Data: " + data.collect());
    System.out.println("Grouped Data: " + groupedData.collect());

// ==========================================================
//    Original Data: [(A,1), (B,2), (A,3), (B,4)]
//    Grouped Data: [(A,[1, 3]), (B,[2, 4])]

    sc.stop();
  }
}

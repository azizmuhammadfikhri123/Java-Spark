package com.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JoinTransformasi {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext("local", "Join Transformation Example");

    List<Tuple2<String, Integer>> rdd1Data = new ArrayList<>();
    rdd1Data.add(new Tuple2<>("A", 1));
    rdd1Data.add(new Tuple2<>("B", 2));
    JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(rdd1Data);

    List<Tuple2<String, Integer>> rdd2Data = new ArrayList<>();
    rdd2Data.add(new Tuple2<>("A", 3));
    rdd2Data.add(new Tuple2<>("C", 4));
    JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(rdd2Data);

    JavaPairRDD<String, Tuple2<Integer, Integer>> joinedRDD = rdd1.join(rdd2);

    System.out.println("RDD 1: " + rdd1.collect());
    System.out.println("RDD 2: " + rdd2.collect());
    System.out.println("Joined Result: " + joinedRDD.collect());
// ========================================================
//    RDD 1: [(A,1), (B,2)]
//    RDD 2: [(A,3), (C,4)]
//    Joined Result: [(A,(1,3))]
  }
}

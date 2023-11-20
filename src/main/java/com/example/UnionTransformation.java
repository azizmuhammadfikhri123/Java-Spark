package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class UnionTransformation {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext("local",  "Union Transformation Example");

    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(6, 7, 8, 9, 10));
    JavaRDD<Integer> unionRdd = rdd1.union(rdd2);

    System.out.println("RDD 1: " + rdd1.collect());
    System.out.println("RDD 2: " + rdd2.collect());
    System.out.println("Union Result: " + unionRdd.collect());

//============================================================
//    RDD 1: [1, 2, 3]
//    RDD 2: [4, 5, 6]
//    Union Result: [1, 2, 3, 4, 5, 6]

    sc.stop();
  }
}

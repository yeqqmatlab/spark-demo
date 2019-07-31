package com.java8.sparkDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yqq on 2019/7/25.
 */
public class SparkRDD04 {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("SparkRDD04").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final int NUM_SAMPLES = 1000000;
        List<Integer> list = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            list.add(i);
        }

        long count = sc.parallelize(list).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x*x + y*y < 1;
        }).count();

        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);


    }

}

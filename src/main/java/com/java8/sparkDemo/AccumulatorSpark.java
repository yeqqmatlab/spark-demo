package com.java8.sparkDemo;


import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;

/**
 * Created by yqq on 2019/12/20.
 */
public class AccumulatorSpark {

    public static void main(String[] args) {


        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("AccumulatorSpark");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        int k = 10;

        Accumulator<Integer> accumulator = sc.accumulator(k);

        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        javaRDD.foreach(x -> accumulator.add(x));

        System.out.println("accumulator-->"+accumulator.value());


    }

}

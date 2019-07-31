package com.java8.sparkDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yqq on 2019/7/18.
 */
public class SparkRDD01 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkRDD01").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Integer> data = Arrays.asList(1, 2, 4, 5, 6, 12);
        JavaRDD<Integer> javaRDD = sc.parallelize(data);

        JavaRDD<Integer> rdd1 = javaRDD.map(x -> x * 2);

        rdd1.foreach(x->System.out.println(x));

    }
}

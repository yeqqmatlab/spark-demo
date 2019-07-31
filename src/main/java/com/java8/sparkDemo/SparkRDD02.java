package com.java8.sparkDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by yqq on 2019/7/25.
 */
public class SparkRDD02 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SparkRDD02").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rddLines = jsc.textFile("hdfs://ip243:8020/zsy/warehouse2/test");

        long count = rddLines.filter(s -> s.contains("25772190889763428")).count();

        System.out.println("count-25772190889763428--->"+count);

    }
}

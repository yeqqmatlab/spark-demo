package com.java8.sparkDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

/**
 * Created by yqq on 2019/7/25.
 * word count
 */
public class SparkRDD03 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkRDD03").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> textFile = sc.textFile("hdfs://ip243:8020/test/words.txt");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile("hdfs://ip243:8020/test/out5");

    }
}



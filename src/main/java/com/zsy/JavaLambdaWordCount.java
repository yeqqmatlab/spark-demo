package com.zsy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;


public class JavaLambdaWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("com.zsy.JavaLambdaWordCount").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //read data
        final JavaRDD<String> lines = jsc.textFile(args[0]);

        //split and flatten
        final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        //put word and one in tuple
        final JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));

        //聚合
        final JavaPairRDD<String, Integer> redcued = wordAndOne.reduceByKey((x, y) -> x + y);

        //调换顺序
        final JavaPairRDD<Integer, String> swaped = redcued.mapToPair(tp -> tp.swap());
        //排序
        final JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        //调换顺序
        final JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());

        //保存
        result.saveAsTextFile(args[1]);

    }
}

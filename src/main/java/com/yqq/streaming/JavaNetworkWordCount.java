package com.yqq.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by yqq on 2019/10/9.
 */
public class JavaNetworkWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("network_word_count");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> wordStream = lines.flatMap(word -> Arrays.asList(word.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = wordStream.mapToPair(t -> new Tuple2<>(t, 1));

        JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey((x1, x2) -> x1 + x2);

        wordsCount.print();

        jssc.start();

        //Thread.sleep(1000*60);

        //jssc.stop(false);

        //jssc.start();

        jssc.awaitTermination();

    }
}

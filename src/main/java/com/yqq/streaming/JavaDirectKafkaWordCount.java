package com.yqq.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by yqq on 2019/10/16.
 */
public class JavaDirectKafkaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "ip247:9092,ip248:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "yqq_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test");

//        JavaInputDStream<ConsumerRecord<String, String>> stream =
//                KafkaUtils.createDirectStream(
//                        jssc,
//                        LocationStrategies.PreferConsistent(),
//                        ConsumerStrategies.Subscribe(topics, kafkaParams)
//                );
//
//        stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }

}

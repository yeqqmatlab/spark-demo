package com.java8.sparkDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yqq on 2019/12/20.
 */
public class broadcastDemoSpark {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("broadcastDemoSpark")
                .master("local[2]")
                .getOrCreate();

//        SparkConf sparkConf = new SparkConf().setAppName("SparkRDD01").setMaster("local[2]");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Integer> list = new ArrayList<>();
        list.add(5);
        list.add(6);
        list.add(7);

        Broadcast<List<Integer>> broadcast = sparkSession.sparkContext().broadcast(list,JavaSparkContext$.MODULE$.fakeClassTag());
        //Broadcast<List<Integer>> broadcast = sc.broadcast(list);


        System.out.println(broadcast.value());

    }
}

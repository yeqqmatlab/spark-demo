package com.yqq.es;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import java.util.Map;

/**
 * Created by yqq on 2019/11/12.
 */
public class ReadDataFromES {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("ElasticSearch-spark")
                .setMaster("local[2]")
                .set("es.es.index.auto.create", "true")
                .set("es.nodes","192.168.1.243")
                .set("es.port","9200")
                .set("es.nodes.wan.only", "true");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter
        JavaRDD<Map<String, Object>> searchRdd = JavaEsSpark.esRDD(jsc, "zsy-paper" ).values();
        for (Map<String, Object> item : searchRdd.collect()) {
            item.forEach((key, value)->{
                System.out.println("search key:" + key + ", search value:" + value);
            });
        }

        sparkSession.stop();

    }
}

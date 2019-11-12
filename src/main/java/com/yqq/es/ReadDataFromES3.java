package com.yqq.es;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 多层json有问题
 * Created by yqq on 2019/11/12.
 */
public class ReadDataFromES3 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("ElasticSearch-spark3")
                .setMaster("local[2]")
                .set("es.es.index.auto.create", "true")
                .set("es.nodes","192.168.1.243")
                .set("es.port","9200")
                .set("es.nodes.wan.only", "true");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> df  = sparkSession
                .read()
                .format("es")
                .load("zsy-paper");

        df.printSchema();

        df.show();

        sparkSession.stop();
    }
}

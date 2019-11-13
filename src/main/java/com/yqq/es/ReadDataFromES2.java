package com.yqq.es;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;
import java.util.Map;

/**
 * Created by yqq on 2019/11/12.
 */
public class ReadDataFromES2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ElasticSearch-spark2")
                .setMaster("local[2]")
                .set("es.es.index.auto.create", "true")
                .set("es.nodes","192.168.1.243")
                .set("es.port","9200")
                .set("es.nodes.wan.only", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, "zsy-etl");
        System.out.println(esRDD.count());
        System.out.println(esRDD.collect().toString());
        for(Tuple2 tuple:esRDD.collect()){
            System.out.print(tuple._1()+"--->");
            System.out.println(tuple._2());
        }

        sc.stop();
    }
}

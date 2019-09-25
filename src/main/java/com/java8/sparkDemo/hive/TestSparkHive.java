package com.java8.sparkDemo.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by yqq on 2019/8/19.
 */
public class TestSparkHive {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("app");


        SparkContext sparkContext = new SparkContext(conf);

        HiveContext hiveContext = new HiveContext(sparkContext);

        hiveContext.table("default.region") // 库名.表名 的格式
                .registerTempTable("region"); // 注册成临时表
        hiveContext.sql("select * from region").show();

        /**
         hiveContext.sql("drop table if exists pokes");
         hiveContext.sql("CREATE TABLE pokes (foo INT, bar STRING)ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' STORED AS TEXTFILE");
         hiveContext.sql("LOAD DATA LOCAL INPATH 'C:\\Users\\r\\Desktop\\kv1.txt' OVERWRITE INTO TABLE pokes");
         **/
        sparkContext.stop();


    }

}

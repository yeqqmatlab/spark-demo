package com.java8.sparkDemo;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

/**
 * Created by yqq on 2019/7/25.
 */
public class SparkRDD05 {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SparkRDD05")
                .master("local[2]")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().json("/Users/yqq/IdeaProjects/spark-demo/src/main/resources/people.json");

        df.show();

        df.printSchema();

        df.select("name").show();

        df.select(col("name"),col("age").plus(1)).show();

        df.filter(col("age").gt(21)).show();

        df.groupBy("age").count().show();

        //Dataset<Tuple2<Integer, Row>> map = df.map((PairFunction<Row, Integer, Row>) row -> new Tuple2<>(Integer.parseInt(row.getInt(0)), row));

        df.createGlobalTempView("people");

        sparkSession.sql("select * from `global_temp`.`people`").show();

        //sparkSession.newSession().sql("select * from global_temp.people").show();
        sparkSession.stop();

    }
}

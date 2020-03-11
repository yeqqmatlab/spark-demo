package com.java8.sparkDemo.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Created by yqq on 2020/2/22.
 */
public class TestParquet1 {

    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("TestParquet1")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/stock_data?characterEncoding=UTF-8")
                .option("dbtable", "stock_data_tab")
                .option("user", "root")
                .option("password", "root")
                .load();

        Dataset<Row> unionDF1 = jdbcDF.union(jdbcDF);

        Dataset<Row> unionDF2 = unionDF1.union(unionDF1);

        Dataset<Row> unionDF3 = unionDF2.union(unionDF2);

        Dataset<Row> unionDF4 = unionDF3.union(unionDF3);

        Dataset<Row> unionDF5 = unionDF4.union(unionDF4);

        Dataset<Row> unionDF6 = unionDF5.union(unionDF5);

        unionDF6.write().mode(SaveMode.Overwrite).parquet("/Users/yqq/parquet3");


        //jdbcDF.show(20);





        spark.stop();



    }
}

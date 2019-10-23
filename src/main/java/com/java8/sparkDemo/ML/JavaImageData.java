package com.java8.sparkDemo.ML;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by yqq on 2019/10/23.
 */
public class JavaImageData {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder().master("local[2]")
                .appName("JavaImageData")
                .getOrCreate();

        final Dataset<Row> imgDF = sparkSession
                .read()
                .format("image")
                .option("dropInvalid", true)
                .load("/Users/yqq/Documents/img/*.jpeg");

        imgDF.select("image.origin", "image.width", "image.height").show(false);


        sparkSession.stop();

    }
}

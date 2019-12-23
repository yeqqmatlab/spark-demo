package com.java8.sparkDemo.saveModes;

import com.java.config.ConfigurationManager;
import com.java.constant.Constants;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.java.constant.Constants.HDFS_DWS_METHOD_SCORE_PATH;

/**
 * Created by yqq on 2019/12/23.
 */
public class ReadHDFS {

    public static final String HDFS_MASTER_URL = ConfigurationManager.getProperty(Constants.HDFS_MASTER_URL);

    Config load = ConfigFactory.load();

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("ReadHDFS")
                .master("local[2]")
                //.config("","")
                .getOrCreate();


        Dataset<Row> methodDF = sparkSession.read().parquet(HDFS_MASTER_URL + HDFS_DWS_METHOD_SCORE_PATH);

        methodDF.show();


        sparkSession.stop();




    }
}

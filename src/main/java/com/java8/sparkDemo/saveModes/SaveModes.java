package com.java8.sparkDemo.saveModes;

import com.java.config.ConfigurationManager;
import com.java.constant.Constants;
import com.java8.sparkJDBC.JDBCHelper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import java.sql.JDBCType;

import static com.java.constant.Constants.HDFS_ODS_PATH;

/**
 * Created by yqq on 2019/12/23.
 */
public class SaveModes {

    public static final String HDFS_MASTER_URL = ConfigurationManager.getProperty(Constants.HDFS_MASTER_URL);

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("ReadHDFS")
                .master("local[2]")
                //.config("","")
                .getOrCreate();

        /*JavaRDD<Row> javaRDD = sparkSession
                .read()
                //.option("delimiter", "[|]")
                .textFile(HDFS_MASTER_URL + HDFS_ODS_PATH + "/date")
                .toJavaRDD().map(line -> {
                    String[] split = line.split("|");

                    return RowFactory.create(split[0], split[1], split[2], split[3]);
                });*/

        /*Dataset<Row> peopleDF = sparkSession.read().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")  //是否自动推断schema
                .option("header", "true")  //是否将第一行作为表头
                .load("src/main/resources/people.csv");

        peopleDF.show();*/

        //csv是 , 分隔的txt文件

        DataFrameReader read = sparkSession.read();
        Dataset<Row> delimiter = read.option("sep", "|").csv(HDFS_MASTER_URL + HDFS_ODS_PATH + "/date");
        delimiter.show();

        //delimiter.write().mode(SaveMode.Append).parquet(HDFS_MASTER_URL+"/test/date");
        JDBCHelper.writeDataToMysql(delimiter,"test_spark_jdbc");
        sparkSession.stop();

    }
}

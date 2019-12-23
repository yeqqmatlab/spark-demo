package com.java8.sparkJDBC;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

/**
 * Created by yqq on 2019/12/23.
 */
public class JDBCHelper {

    public static void writeDataToMysql(Dataset<Row> examResDF, String tableName) {

        String url = "jdbc:mysql://192.168.1.210:3307/zsy_data?characterEncoding=UTF-8";
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", "zsy");// 设置用户名
        connectionProperties.setProperty("password", "lc12345");// 设置密码

        try {
            examResDF
                    .write()
                    .mode(SaveMode.Overwrite)
                    .option("truncate","true") //不删除表结构
                    .option("batchsize","1000") //批处理 默认1000
                    .jdbc(url,tableName,connectionProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

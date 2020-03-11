package com.java8.sparkDemo.sql;

import com.java8.sparkJDBC.JDBCHelper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by yqq on 2020/2/22.
 * test DF.select("col1","col2")
 */
public class TestParquet2 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("TestParquet2")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> parquetFileDF = spark.read().parquet("/Users/yqq/parquet3");
        parquetFileDF.createOrReplaceTempView("test_stock");

        //Dataset<Row> selectDF = parquetFileDF.select("stock_code", "stock_name", "now_price");
        //selectDF.createOrReplaceTempView("test_stock");

        StringBuffer sql = new StringBuffer();

        sql.append("select stock_code,stock_name,now_price,buy_one_num from test_stock");

        Dataset<Row> sqlDF = spark.sql(sql.toString());

        JDBCHelper.writeDataToMysql(sqlDF,"test_1");
        //long count = parquetFileDF.count();
        //System.out.println("count-->"+count);






        spark.stop();
    }

}

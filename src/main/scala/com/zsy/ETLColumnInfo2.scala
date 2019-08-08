package com.zsy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by yqq on 2019/8/6.
  */
object ETLColumnInfo2 {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("ETLColumnInfo")
      .master("local[2]")
      .getOrCreate()

    val lines:RDD[String] = session.sparkContext.textFile("/Users/yqq/Downloads/column_file/column_info.txt")
    val words: RDD[String] = lines.flatMap(_.split("[,]"))
    val newRDD: RDD[String] = words.map(word => "public static Column<? extends Object> "+word.toUpperCase+" = new Column(0, "+"\""+word+"\""+", null"+");")
    newRDD.saveAsTextFile("/Users/yqq/Downloads/column_file/out2")

    session.stop()
  }

}

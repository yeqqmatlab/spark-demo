package com.zsy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by yqq on 2019/8/5.
  */
object ETLColumnInfo {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("ETLColumnInfo")
      .master("local[2]")
      .getOrCreate()

    val lines:RDD[String] = session.sparkContext.textFile("/Users/yqq/Downloads/column_file/column_info.txt")
    //val lines: RDD[String] = session.sparkContext.textFile(args(0))
    val words: RDD[String] = lines.flatMap(_.split("[,]"))
    val newRDD: RDD[String] = words.map(word => word.toUpperCase+"(0, "+"\""+word+"\""+", null"+"),")
    newRDD.saveAsTextFile("/Users/yqq/Downloads/column_file/out1")


    session.stop()

  }

}

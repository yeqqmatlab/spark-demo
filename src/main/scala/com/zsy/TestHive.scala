package com.zsy

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by yqq on 2019/8/16.
  */
object TestHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("TestHive").setMaster("local")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    spark.sql("show databases").collect().foreach(println)
  }
}

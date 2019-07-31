package com.zsy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object ScalaWordCount {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置配置信息
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local")
    //sc是SparkContext，他是spark程序执行的入口
    val sc = new SparkContext(conf)
    //编写spark程序
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).saveAsTextFile(args(1))

    //指定从哪里读取数据并生成RDD
    val lines: RDD[String] = sc.textFile(args(0))
    //将一行内容进行切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词和一放到一个元组里
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //继续聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //保存数据
    sorted.saveAsTextFile(args(1))
    //释放资源
    sc.stop()


  }
}

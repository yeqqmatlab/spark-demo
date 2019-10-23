package com.zsy.ml

import org.apache.spark.ml.stat.Correlation
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yqq on 2019/10/22.
  */
object StatisticsCorrLearning {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("StatisticsCorrLearning")
    val sc = new SparkContext(conf)


    val rdd7 = sc.parallelize(Array(170.0, 150.0, 210.0,180.0,160.0))
    val rdd8 = sc.parallelize(Array(180.0, 165.0, 190.0,168.0,172.0))
    println("rdd:")
    rdd7.foreach(each => print(each + " "))
    println("\nrdd:")
    rdd8.foreach(each => print(each + " "))
    val correlationPearson7 = Statistics.corr(rdd7, rdd8) //计算不同数据之间的相关系数:皮尔逊
    println("\ncorrelationPearson：" + correlationPearson7) //打印结果
    //correlationPearson：0.8171759569273293

    val correlationSpearman7 = Statistics.corr(rdd7, rdd8, "spearman") //使用斯皮尔曼计算不同数据之间的相关系数
    println("correlationSpearman：" + correlationSpearman7) //打印结果
    //correlationSpearman：0.6999999999999998



  }

}

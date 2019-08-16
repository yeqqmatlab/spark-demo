package com.java8.sparkDemo.ML

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.rdd.RDD

/**
  * 关联算法
  * Created by yqq on 2019/8/9.
  */
object FPGrowthDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("FPGrowthDemo")
      .master("local[2]")
      .config("spark.sql.warehouse.dir","/Users/yqq/Desktop/sparktest")
      .getOrCreate()

    import spark.implicits._
    /*val dataset = spark.createDataset(Seq(
      "1 2 5",
      "1 2 3 5",
      "1 2  ")
    ).map(t => t.split(" ")).toDF("items")*/

    //val dataframe: DataFrame = spark.read.json("/Users/yqq/Desktop/mysql_data/paper_topic_relation.json")
    val methodIdsRDD: RDD[String] = spark.sparkContext.textFile("/Users/yqq/Desktop/mysql_data/paper_topic_relation.txt")

    val dataset: DataFrame = methodIdsRDD.map(t => t.split("[,]")).toDF("items")

    val fpgroth=new FPGrowth().setItemsCol("items").setMinSupport(0.05).setMinConfidence(0.02)
    val model=fpgroth.fit(dataset)
    // Display frequent itemsets.
    model.freqItemsets.show()

    // Display generated association rules.
    model.associationRules.show()
    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    model.transform(dataset).show()
    // $example off$

    spark.stop()

  }

}

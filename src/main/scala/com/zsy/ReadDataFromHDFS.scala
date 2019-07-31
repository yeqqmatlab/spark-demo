package com.zsy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by yqq on 2019/5/28.
  */
object ReadDataFromHDFS {

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .appName("ETL_ReadDataFromHDFS")
      .master("local[*]")
      .getOrCreate()

    val lines:RDD[String] = session.sparkContext.textFile("hdfs://ip243:8020/zsy/warehouse2/test")

    val arrRDD: RDD[((String, String, String, String, String, String, String), String)] = lines.filter(_.split("[|]").length == 8).map(line => {

      val fields = line.split("[|]")

      val field1 = fields(0)
      val field2 = fields(1)
      val field3 = fields(2)
      val field4 = fields(3)
      val field5 = fields(4)
      val field6 = fields(5)
      val field7 = fields(6)
      val field8 = fields(7)
      ((field1, field2, field3, field4, field5, field6, field7), field8)
    })
    //val lines: DataFrame = session.read.option("delimiter", "|").option("header", false).load("hdfs://ip243:8020/zsy/warehouse2/test")


    /**
    val res: RDD[(String, String, String, String)] = arrRDD.flatMapValues(_.split("[,]")).map(ele => {

      val value1 = ele._1._1
      val value2 = ele._1._2
      val value3 = ele._1._3
      val value4 = ele._2
      (value1, value2, value3, value4)
    })

    res.saveAsTextFile("hdfs://ip243:8020/zsy/warehouse3/")
    */

    /**
    val res = arrRDD.flatMapValues(_.split("[,]")).map(ele => {

      val value1 = ele._1._1
      val value2 = ele._1._2
      val value3 = ele._1._3
      val value4 = ele._2
      Row(value1, value2, value3, value4)
    })

    val schema = StructType(
      List(
        StructField("id", StringType, true),
        StructField("name", StringType, true),
        StructField("age", StringType, true),
        StructField("fv", StringType, true)
      )
    )

    val df: DataFrame = session.createDataFrame(res, schema)

    df.write.parquet("hdfs://ip243:8020/zsy/warehouse4/")
      */

    val res = arrRDD.flatMapValues(_.split("[,]")).map(ele => {
      val value1 = ele._1._1.toString
      val value2 = ele._1._2.toString
      val value3 = ele._1._3.toString
      val value4 = ele._1._4.toString
      val value5 = ele._1._5.toString
      val value6 = ele._1._6.toString
      val value7 = ele._1._7.toString
      val value8 = ele._2.toString
      value1+"|"+value2+"|"+value3+"|"+value4+"|"+value5+"|"+value6+"|"+value7+"|"+value8
    })

    //if path exists and then delete it
    val path = "hdfs://ip243:8020/zsy/warehouse5"
    val hdfs= FileSystem.get(new java.net.URI("hdfs://ip243:8020"),new Configuration())
    if(hdfs.exists(new Path(path))){
      println("delete exits files")
      hdfs.delete(new Path(path),true)
    }


    res
      //.coalesce(1,true)
      .saveAsTextFile(path)

    session.close()

  }

}

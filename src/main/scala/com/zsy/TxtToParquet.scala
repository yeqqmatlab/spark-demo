package com.zsy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by yqq on 2019/6/4.
  */
object TxtToParquet {


  def main(args: Array[String]): Unit = {

    // 0 校验参数个数
    if (args.length != 2) {
      println(
        """
          |参数：
          | inputPath
          | outputPath
        """.stripMargin)
      sys.exit()
    }

    // 1 接受程序参数
    val Array(inputPath,outputPath) = args


    val session = SparkSession
      .builder()
      .appName("ETL_TxtToParquet")
      //.master("local[*]")
      .getOrCreate()

    //val inputPath="hdfs://ip243:8020//zsy/warehouse/exam_student_topic/zsy_exam_student_topic-zsy_school_1003.txt"
    val lines:RDD[String] = session.sparkContext.textFile(inputPath)

    val arrRDD: RDD[((String, String, String, String, String, String, String, String, String, String), String)] = lines.filter(_.split("[|]").length == 11).map(line => {

      val fields = line.split("[|]")

      val field0 = fields(0)
      val field1 = fields(1)
      val field2 = fields(2)
      val field3 = fields(3)
      val field4 = fields(4)
      val field5 = fields(5)
      val field6 = fields(6)
      val field7 = fields(7)
      val field8 = fields(8)
      val field9 = fields(9)
      val field10 = fields(10)

      ((field0, field1, field2, field3, field4, field5, field6, field7, field8, field9), field10)
    })

    val res: RDD[Row] = arrRDD.flatMapValues(_.split("[,]")).filter(_._2.trim != "0").map(ele => {

      val value1 = ele._1._1
      val value2 = ele._1._2
      val value3 = ele._1._3
      val value4 = ele._1._4
      val value5 = ele._1._5.trim.toLong
      val value6 = ele._1._6.trim.toInt
      val value7 = ele._1._7.trim.toInt
      val value8 = ele._1._8.trim.toDouble
      val value9 = ele._1._9.trim.toDouble
      val value10 = ele._1._10.trim.toDouble
      val value11 = ele._2.trim.toInt
      Row(value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11)
    })


    val schema = StructType(
      List(
        StructField("exam_group_id", StringType, true),
        StructField("id", StringType, true),
        StructField("school_id", StringType, true),
        StructField("student_id", StringType, true),
        StructField("topic_id", LongType, true),
        StructField("type", IntegerType, true),
        StructField("is_right", IntegerType, true),
        StructField("scoring", DoubleType, true),
        StructField("score", DoubleType, true),
        StructField("scoring_ratio", DoubleType, true),
        StructField("method_id", IntegerType, true)
      )
    )

    val df: DataFrame = session.createDataFrame(res, schema)

    //if path exists and then delete it
    //val outputPath = "hdfs://ip243:8020/zsy/warehouse3"

    val hdfs = FileSystem.get(new java.net.URI("hdfs://ip243:8020"),new Configuration())
    if(hdfs.exists(new Path(outputPath))){
      println("delete exits files")
      hdfs.delete(new Path(outputPath),true)
    }

    df.write.parquet(outputPath)

    session.close()

  }


}

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
object TxtToParquet2 {


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
      .appName("ETL_TxtToParquet2")
      //.master("local[*]")
      .getOrCreate()

    //val inputPath="hdfs://ip243:8020//zsy/warehouse/exam_student_topic/zsy_exam_student_topic-zsy_school_1003.txt"
    val lines:RDD[String] = session.sparkContext.textFile(inputPath)

    val res = lines.filter(_.split("[|]").length == 11).map(line => {

      val fields = line.split("[|]")

      val field0 = fields(0)
      val field1 = fields(1)
      val field2 = fields(2)
      val field3 = fields(3)
      val field4 = fields(4).trim.toLong
      val field5 = fields(5).trim.toInt
      val field6 = fields(6).trim.toInt
      val field7 = fields(7).trim.toDouble
      val field8 = fields(8).trim.toDouble
      val field9 = fields(9).trim.toDouble
      val field10 = fields(10).trim

      Row(field0, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10)
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
        StructField("method_id", StringType, true)
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

package com.zsy

//import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by yqq on 2019/5/10.
  */
object ETLReadDataFromMysql {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("ReadDataFromMysql")
      .master("local[*]")
      .getOrCreate()

    val logs: DataFrame = session.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "logs",
        "user" -> "root",
        "password" -> "root")
    ).load()

    //创建Properties存储数据库相关属性
   /* val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    logs.where(logs.col("id") <= 3).write.mode("append").jdbc("jdbc:mysql://localhost:3306/bigdata", "bigdata.logs", prop)
*/

    //logs.where(logs.col("id") <= 3)

    logs.createTempView("v_logs")

    val res: DataFrame = session.sql("select * from v_logs where id > 4")


    val count: Long = res.count()
    println(count)
    res.show()

    session.close()

  }

}

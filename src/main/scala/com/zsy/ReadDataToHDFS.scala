package com.zsy

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by yqq on 2019/5/28.
  */
object ReadDataToHDFS {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("ReadDataFromMysql")
      .master("local[*]")
      .getOrCreate()

    val logs: DataFrame = session.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://192.168.1.210:3307/zsy_school_838?tinyInt1isBit=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "student_paper_topic_rs",
        "user" -> "zsy",
        "password" -> "lc12345")
    ).load()

    logs.createTempView("v_student_paper_topic_rs")

    val df: DataFrame = session.sql("select * from v_student_paper_topic_rs where id = 25937518122568172")

    val count: Long = df.count()
    println(count)
    df.show()

    val data: DataFrame = df.select("id","student_id","paper_id","topic_id")

   /* val dataset: Dataset[(String, String, String, String)] = data.map(row => {
      (row.get(0).toString, row.get(1).toString, row.get(2).toString, row.get(3).toString)
    })

    val conn = getConnection()
    val table = conn.getTable(TableName.valueOf("test_spark"))
    dataset.foreach( vo => {
      val put = new Put(Bytes.toBytes(vo._1))



    })*/



    session.close()

  }

  def getConnection(): Connection = {
    val load = ConfigFactory.load()
    val HBaseConf = HBaseConfiguration.create()
    HBaseConf.set(HConstants.ZOOKEEPER_QUORUM, load.getString("hbase.zookeeper.quorum"))
    HBaseConf.set(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, "2181")
    ConnectionFactory.createConnection(HBaseConf)
  }

}

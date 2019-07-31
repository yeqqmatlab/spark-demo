package com.zsy

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yqq on 2019/5/8.
  */
object SparkPut {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    .setMaster("local[2]")
    val sc = new SparkContext(conf)

    try {
      val rdd = sc.makeRDD(1 to 100, 4)
      // column family
      val family1 = Bytes.toBytes("f1")
      val family2 = Bytes.toBytes("f2")
      // column
      val column1 = Bytes.toBytes("name")
      val column2 = Bytes.toBytes("age")
      val column3 = Bytes.toBytes("last_name")
      println("count is :" + rdd.count())
      rdd.take(5).foreach(println)

      rdd.foreachPartition(list => {
        //get conn
        val conn = getConnection()
        val table = conn.getTable(TableName.valueOf("t2"))
        list.foreach(i => {
          val put = new Put(Bytes.toBytes("rowkey"+i))
          put.addColumn(family1,column1,Bytes.toBytes("tom"+i))
          put.addColumn(family1,column2,Bytes.toBytes(""+i))
          put.addColumn(family2,column3,Bytes.toBytes("jack"+i))
          table.put(put)
        })
        table.close()
        conn.close()
      })
    } finally {
      sc.stop()
    }
  }

  def getConnection(): Connection = {
    val load = ConfigFactory.load()
    val HBaseConf = HBaseConfiguration.create()
    HBaseConf.set(HConstants.ZOOKEEPER_QUORUM, load.getString("hbase.zookeeper.quorum"))
    HBaseConf.set(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, "2181")
    ConnectionFactory.createConnection(HBaseConf)
  }

}

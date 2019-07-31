package com.zsy

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by yqq on 2019/6/13.
  */
object ReadDataFromHBase {

  def main(args: Array[String]): Unit = {
   /* val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)*/

    val conn = getConnection()
    val table = conn.getTable(TableName.valueOf("t2"))
    val get = new Get(Bytes.toBytes("rowkey"+1))
    val result = table.get(get)
    System.out.print("--->"+result)
    table.close()
    conn.close()


  }


  def getConnection(): Connection = {
    val load = ConfigFactory.load()
    val HBaseConf = HBaseConfiguration.create()
    HBaseConf.set(HConstants.ZOOKEEPER_QUORUM, load.getString("hbase.zookeeper.quorum"))
    HBaseConf.set(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, "2181")
    ConnectionFactory.createConnection(HBaseConf)
  }

}

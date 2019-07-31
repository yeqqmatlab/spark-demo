package com.zsy;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * Created by yqq on 2019/6/14.
 */
public class ZSYHBaseConnectionFactory {

    /**
     * conn hbase
     * @return
     */
    public static Connection getConnection(){
        Config load = ConfigFactory.load();
        Connection connection = null;
        Configuration HBaseConf = HBaseConfiguration.create();
        try {
            HBaseConf.set(HConstants.ZOOKEEPER_QUORUM, load.getString("hbase.zookeeper.quorum"));
            HBaseConf.set(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, "2181");
            connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(HBaseConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }
}

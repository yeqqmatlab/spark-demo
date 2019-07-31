package com.zsy;

import javafx.scene.text.TextAlignment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

/**
 * Created by yqq on 2019/6/14.
 */
public class GetExample {

    public static void main(String[] args) throws IOException {
        //get conn
        Connection connection = ZSYHBaseConnectionFactory.getConnection();
        if (connection == null){
            new Throwable("no getConnection!!!");
        }
        //get table
        Table table = connection.getTable(TableName.valueOf("t2"));
        byte[] rowkey = Bytes.toBytes("java_row1");
        byte[] f1 = Bytes.toBytes("f1");
        byte[] name = Bytes.toBytes("name");

        Get get1 = new Get(rowkey);

        get1.addColumn(f1, name);
        boolean exists = table.exists(get1);
        System.out.println(exists);
        Result result = table.get(get1);
        byte[] value = result.getValue(f1, name);
        for (byte b : value) {
            System.out.println("b-->"+b);
        }

        System.out.println("value--->"+Bytes.toString(value));


        System.out.println("-------------------------------------------------");
        char[] chars = getChars(value);
        for (char ch : chars) {
            System.out.println("ch--->"+ch);
        }
        System.out.println("chars--->"+chars);

        byte[] value1 = result.value();
        System.out.println("value1--->"+Bytes.toString(value1));


        table.close();
        connection.close();


    }


    public static char[] getChars(byte[] bytes) {
        Charset cs = Charset.forName("UTF-8");
        ByteBuffer bb = ByteBuffer.allocate(bytes.length);
        bb.put(bytes);
        bb.flip();
        CharBuffer cb = cs.decode(bb);
        return cb.array();
    }

}

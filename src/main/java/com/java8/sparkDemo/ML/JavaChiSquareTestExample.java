package com.java8.sparkDemo.ML;

import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.stat.ChiSquareTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * 卡方分布
 * Created by yqq on 2019/10/23.
 */
public class JavaChiSquareTestExample {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("ChiSquareTestExample")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, Vectors.dense(0.5, 10.0)),
                RowFactory.create(0.0, Vectors.dense(1.5, 20.0)),
                RowFactory.create(1.0, Vectors.dense(1.5, 30.0)),
                RowFactory.create(0.0, Vectors.dense(3.5, 30.0)),
                RowFactory.create(0.0, Vectors.dense(3.5, 40.0)),
                RowFactory.create(1.0, Vectors.dense(3.5, 40.0))
        );

        final StructField field = new StructField("label", DataTypes.DoubleType, false, Metadata.empty());
        final StructField field2 = new StructField("features", new VectorUDT(), false, Metadata.empty());
        StructField[] arr = new StructField[]{field,field2};

        final StructType schema = new StructType(arr);

        final Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        Row r = ChiSquareTest.test(df, "features", "label").head();
        System.out.println("pValues: " + r.get(0).toString());
        System.out.println("degreesOfFreedom: " + r.getList(1).toString());
        System.out.println("statistics: " + r.get(2).toString());



        sparkSession.stop();
    }

}

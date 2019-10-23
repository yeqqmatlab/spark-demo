package com.java8.sparkDemo.ML;

import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * Created by yqq on 2019/10/22.
 */
public class JavaCorrelationExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder().master("local[2]")
                .appName("JavaCorrelationExample")
                .getOrCreate();

        /**
         * dense:密集向量
         * sparse:稀疏向量
         */
        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{1.0, -2.0})),
                RowFactory.create(Vectors.dense(4.0, 5.0, 0.0, 3.0)),
                RowFactory.create(Vectors.dense(6.0, 7.0, 0.0, 8.0)),
                RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{9.0, 1.0}))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });

        Dataset<Row> dataFrame = sparkSession.createDataFrame(data, schema);

        /**
         * Pearson相关系数是用来衡量两个数据集合是否在一条线上面，用来衡量定距变量间的线性关系
         */
        final Row r1 = Correlation.corr(dataFrame, "features").head();
        System.out.println("Pearson correlation matrix:\n" + r1.get(0).toString());

        /**
         * spearman相关系数是衡量两个变量的依赖性的非参数指标。
         */
        Row r2 = Correlation.corr(dataFrame, "features", "spearman").head();
        System.out.println("Spearman correlation matrix:\n" + r2.get(0).toString());

        /**
         * 每一行都有四个数，代表当前第几个向量与Seq中的4个向量的相关性，
         * 比如皮尔森的第一行结果1.0 0.055641488407465814 NaN 0.4004714203168137与自己的相关性是1.0，
         * 与第二个相关性为0.055641488407465814,与第三个无法计算相关性，
         * 与第四个相关性0.055641488407465814。
         */




        sparkSession.stop();
    }
}

package com.java8.sparkDemo.ML;

import org.apache.spark.examples.ml.JavaDocument;
import org.apache.spark.examples.ml.JavaLabeledDocument;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.List;

/**
 * ML pipeline 处理流
 * Created by yqq on 2019/10/23.
 */
public class JavaPipelineExample {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("PipelineExample")
                .getOrCreate();

        // prepare training documents, which are labeled
        final Dataset<Row> training = sparkSession.createDataFrame(Arrays.asList(
                new JavaLabeledDocument(0L, "a b c d e spark", 1.0),
                new JavaLabeledDocument(1L, "b d", 0.0),
                new JavaLabeledDocument(2L, "spark f g h", 1.0),
                new JavaLabeledDocument(3L, "hadoop mapreduce", 0.0)
        ), JavaLabeledDocument.class);

        // Configure an ML pipeline,
        // which consists of three stages: tokenizer, hashingTF, and lr.
        // 分词器
        final Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");

        // hash函数映射
        final HashingTF hashingTF = new HashingTF()
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features")
                .setNumFeatures(1000);

        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.001);

        final Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, hashingTF, lr});

        // Fit the pipeline to training documents.
        final PipelineModel model = pipeline.fit(training);

        // Prepare test documents, which are unlabeled.
        Dataset<Row> test = sparkSession.createDataFrame(Arrays.asList(
                new JavaDocument(4L, "spark i j k"),
                new JavaDocument(5L, "l m n"),
                new JavaDocument(6L, "spark hadoop spark"),
                new JavaDocument(7L, "apache hadoop")
        ), JavaDocument.class);

        final Dataset<Row> predictions = model.transform(test);

        predictions.show();

        final List<Row> rows = predictions
                .select("id", "text", "probability", "prediction")
                .collectAsList();

        for (Row r : rows) {
            System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
        }

        sparkSession.stop();
    }
}

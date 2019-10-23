package com.java8.sparkDemo.ML;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
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
 * Created by yqq on 2019/10/23.
 */
public class JavaEstimatorTransformerParamExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("JavaEstimatorTransformerParamExample")
                .getOrCreate();


        // $example on$
        // Prepare training data.
        List<Row> dataTraining = Arrays.asList(
            RowFactory.create(1.0, Vectors.dense(0.0, 1.1, 0.1)),
            RowFactory.create(0.0, Vectors.dense(2.0, 1.0, -1.0)),
            RowFactory.create(0.0, Vectors.dense(2.0, 1.3, 1.0)),
            RowFactory.create(1.0, Vectors.dense(0.0, 1.2, -0.5))
        );

        /**
         * VectorUDT 用户定义的向量类型 UserDefinedType[Vector]
         */
        StructType schema = new StructType(new StructField[]{
            new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("features", new VectorUDT(), false, Metadata.empty())
        });

        final Dataset<Row> training = spark.createDataFrame(dataTraining, schema);

        // create a LogisticRegression 逻辑回归 instance ,This instance is an Estimator 算子

        final LogisticRegression lr = new LogisticRegression();

        System.out.println("LogisticRegression parameters:\n"+lr.explainParams()+"\n");

        // set params using setter methods

        lr.setMaxIter(10).setRegParam(0.01);

        final LogisticRegressionModel LogisticRegressionModel = lr.fit(training);

        System.out.println("LogisticRegressionModel was fit using parameters: " + LogisticRegressionModel.parent().extractParamMap());

        final ParamMap paramMap = new ParamMap()
                .put(lr.maxIter().w(20))
                .put(lr.maxIter(), 30)
                .put(lr.regParam().w(0.1), lr.threshold().w(0.55));

        ParamMap paramMap2 = new ParamMap()
                .put(lr.probabilityCol().w("myProbability"));  // Change output column name
        ParamMap paramMapCombined = paramMap.$plus$plus(paramMap2);

        LogisticRegressionModel model2 = lr.fit(training, paramMapCombined);
        System.out.println("Model 2 was fit using parameters: " + model2.parent().extractParamMap());

        // Prepare test documents.
        List<Row> dataTest = Arrays.asList(
                RowFactory.create(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
                RowFactory.create(0.0, Vectors.dense(3.0, 2.0, -0.1)),
                RowFactory.create(1.0, Vectors.dense(0.0, 2.2, -1.5)),
                RowFactory.create(0.0, Vectors.dense(0.0, 1.2, 0.3))
        );
        Dataset<Row> test = spark.createDataFrame(dataTest, schema);

        final Dataset<Row> results = model2.transform(test);

        results.show();

        Dataset<Row> rows = results.select("features", "label", "myProbability", "prediction");
        for (Row r: rows.collectAsList()) {
            System.out.println("(" + r.get(0) + ", " + r.get(1) + ") -> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
        }

        spark.stop();

    }
}

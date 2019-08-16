package com.java8.sparkDemo.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Seq;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yqq on 2019/8/15.
 */
public class JavaUserDefinedUntypedAggregation {

    public static class MyAverage extends UserDefinedAggregateFunction{

        private StructType inputSchema;
        private StructType bufferSchema;

        public MyAverage() {
            //super();
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn",DataTypes.LongType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);

        }

        public StructType inputSchema(){
            return inputSchema;
        }

        public StructType bufferSchema(){
            return bufferSchema;
        }

        public DataType dataType() {
            return DataTypes.DoubleType;
        }

        public boolean deterministic() {
            return true;
        }

        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0L);
            buffer.update(1, 0L);
        }

        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                long updatedSum = buffer.getLong(0) + input.getLong(0);
                long updatedCount = buffer.getLong(1) + 1;
                buffer.update(0, updatedSum);
                buffer.update(1, updatedCount);
            }
        }

        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
            long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
            buffer1.update(0, mergedSum);
            buffer1.update(1, mergedCount);
        }

        public Double evaluate(Row buffer) {
            return ((double) buffer.getLong(0)) / buffer.getLong(1);
        }

    }

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("ava Spark SQL user-defined DataFrames aggregation example")
                .master("local[2]")
                .getOrCreate();

        sparkSession.udf().register("myAverage", new MyAverage());

        Dataset<Row> df = sparkSession.read().json("/Users/yqq/IdeaProjects/spark-demo/src/main/resources/employees.json");
        df.createOrReplaceTempView("employees");
        df.show();

        //Dataset<Row> res = sparkSession.sql("select avg(salary) as avg_salary from employees");
        Dataset<Row> res = sparkSession.sql("select MyAverage(salary) as avg_salary from employees");
        res.show();

        sparkSession.stop();

    }
}

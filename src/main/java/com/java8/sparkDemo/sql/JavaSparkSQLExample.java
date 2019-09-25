package com.java8.sparkDemo.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import javax.validation.constraints.Null;
import static org.apache.spark.sql.functions.col;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by yqq on 2019/8/8.
 */
public class JavaSparkSQLExample {

    public static class Person implements Serializable{
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args)throws AnalysisException {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkRDD05")
                .master("local[2]")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        //runBasicDataFrameExample(spark);
        //runDatasetCreationExample(spark);
        //runInferSchemaExample(spark);
        runProgrammaticSchemaExample(spark);



        spark.stop();
    }


    private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {

        // $example on:create_df$
        Dataset<Row> df = spark.read().json("/Users/yqq/IdeaProjects/spark-demo/src/main/resources/people.json");

        // Displays the content of the DataFrame to stdout
        df.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
        // $example off:create_df$

        // $example on:untyped_ops$
        // Print the schema in a tree format
        df.printSchema();
        // root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)

        // Select only the "name" column
        df.select("name").show();
        // +-------+
        // |   name|
        // +-------+
        // |Michael|
        // |   Andy|
        // | Justin|
        // +-------+

        // Select everybody, but increment the age by 1
        df.select(col("name"), col("age").plus(1)).show();
        // +-------+---------+
        // |   name|(age + 1)|
        // +-------+---------+
        // |Michael|     null|
        // |   Andy|       31|
        // | Justin|       20|
        // +-------+---------+

        // Select people older than 21
        df.filter(col("age").gt(21)).show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 30|Andy|
        // +---+----+

        // Count people by age
        df.groupBy("age").count().show();
        // +----+-----+
        // | age|count|
        // +----+-----+
        // |  19|    1|
        // |null|    1|
        // |  30|    1|
        // +----+-----+
        // $example off:untyped_ops$

        // $example on:run_sql$
        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
        // $example off:run_sql$

        // $example on:global_temp_view$
        // Register the DataFrame as a global temporary view
        df.createGlobalTempView("people");

        // Global temporary view is tied to a system preserved database `global_temp`
        spark.sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+

        // Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
        // $example off:global_temp_view$

    }

    private static void runDatasetCreationExample(SparkSession spark) {

        Person person = new Person();
        person.setAge(23);
        person.setName("jack");

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        //java object 不能再网络传播 需要序列化
        Dataset<Person> dataset1 = spark.createDataset(Collections.singletonList(person), personEncoder);
        dataset1.show();

        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> dataset2 = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);

        Dataset<Integer> ds = dataset2.map((MapFunction<Integer, Integer>) x -> x + 1, integerEncoder);

        ds.show();

        Dataset<Person> personDataFrame = spark.read().json("/Users/yqq/IdeaProjects/spark-demo/src/main/resources/people.json").as(personEncoder);

        personDataFrame.show();


    }

    private static void runInferSchemaExample(SparkSession spark) {

        JavaRDD<Person> personJavaRDD = spark.read()
                .textFile("/Users/yqq/IdeaProjects/spark-demo/src/main/resources/people.txt")
                .javaRDD()
                .map(lines -> {
                    String[] splits = lines.split("[,]");
                    Person person = new Person();
                    person.setName(splits[0]);
                    person.setAge(Integer.parseInt(splits[1].trim()));
                    return person;
                });

        Dataset<Row> dataFrame = spark.createDataFrame(personJavaRDD, Person.class);

        dataFrame.createOrReplaceTempView("person");

        Dataset<Row> teenagersDF = spark.sql(" select name from person where age between 13 and 19");

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> nameDF = teenagersDF.map((MapFunction<Row,String>)row -> "Name: " + row.getString(0), stringEncoder);
        nameDF.show();

    }

    private static void runProgrammaticSchemaExample(SparkSession spark) {
        JavaRDD<String> stringJavaRDD = spark.sparkContext()
                .textFile("/Users/yqq/IdeaProjects/spark-demo/src/main/resources/people.txt", 1)
                .toJavaRDD();

        String[] schemaString = {"name", "age"};

        List<StructField> fields = new ArrayList<>();

        for (String fieldName : schemaString) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        /*StructField field1 = DataTypes.createStructField(schemaString[0], DataTypes.StringType, true);
        fields.add(field1);

        StructField field2 = DataTypes.createStructField(schemaString[1], DataTypes.IntegerType, true);
        fields.add(field2);*/


        StructType schema = DataTypes.createStructType(fields);

        // convert records of the RDD<String> to RDD<Row>
        JavaRDD<Row> rowRDD = stringJavaRDD.map(line -> {
            String[] split = line.split("[,]");
            Row row = RowFactory.create(split[0], split[1].trim());
            return row;
        });

        Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);

        dataFrame.createOrReplaceTempView("person");



        Dataset<Row> res = spark.sql("select * from person");

        res.show();
        /*Dataset<String> nameDS = res.map((MapFunction<Row, String>) row -> "name:" + row.getString(0), Encoders.STRING());

        nameDS.show();*/


    }


}

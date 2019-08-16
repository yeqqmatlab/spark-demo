package com.java8.sparkDemo.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yqq on 2019/8/15.
 */
public class JavaSQLDataSourceExample {

    public static class Square implements Serializable{
        private int value;
        private int square;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public int getSquare() {
            return square;
        }

        public void setSquare(int square) {
            this.square = square;
        }
    }

    public static class Cube implements Serializable {
        private int value;
        private int cube;

        // Getters and setters...
        // $example off:schema_merging$
        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public int getCube() {
            return cube;
        }

        public void setCube(int cube) {
            this.cube = cube;
        }
    }

    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .master("local[2]")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        //runBasicDataSourceExample(spark);

        //runBasicParquetExample(spark);

        runParquetSchemaMergingExample(spark);

    }

    private static void runBasicDataSourceExample(SparkSession spark) {

        Dataset<Row> usersDF = spark.read().load("src/main/resources/users.parquet");
        /*usersDF.select("name", "favorite_color").write().save("namesAndFavColors2.parquet");*/

        Dataset<Row> peopleDF =
                spark.read().format("json").load("src/main/resources/people.json");
        //peopleDF.select("name", "age").write().format("parquet").save("namesAndAges2.parquet");

        /*Dataset<Row> peopleDFCsv = spark.read().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("src/main/resources/people.csv");

        peopleDFCsv.show();*/

       /* peopleDF.write()
                .bucketBy(42, "name")
                .sortBy("age")
                .saveAsTable("people_bucketed");*/

        usersDF.write()
                .partitionBy("favorite_color")
                .format("parquet")
                .save("namesPartByColor.parquet");


    }

    private static void runBasicParquetExample(SparkSession spark) {

        Dataset<Row> peopleDF = spark.read().json("src/main/resources/people.json");

        peopleDF.write().parquet("people.parquet");

        Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");

        parquetFileDF.createOrReplaceTempView("peopleTable");

        Dataset<Row> namesDF = spark.sql("SELECT name FROM peopleTable WHERE age BETWEEN 13 AND 19");

        Dataset<String> nameDF = namesDF.map(
                (MapFunction<Row, String>) row -> "name:"+row.getString(0), Encoders.STRING());

        nameDF.show();

    }

    private static void runParquetSchemaMergingExample(SparkSession spark) {

        List<Square> squares = new ArrayList<>();
        for (int value = 1; value <= 5; value++) {
            Square square = new Square();
            square.setValue(value);
            square.setSquare(value * value);
            squares.add(square);
        }

        Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
        squaresDF.write().parquet("data/test_table/key=1");

        List<Cube> cubes = new ArrayList<>();
        for (int value = 6; value <= 10; value++) {
            Cube cube = new Cube();
            cube.setValue(value);
            cube.setCube(value * value * value);
            cubes.add(cube);
        }

        Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
        cubesDF.write().parquet("data/test_table/key=2");

        Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("data/test_table");
        mergedDF.printSchema();




    }


}

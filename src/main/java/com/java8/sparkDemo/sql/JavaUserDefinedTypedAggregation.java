package com.java8.sparkDemo.sql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/**
 * Created by yqq on 2019/8/15.
 */
public class JavaUserDefinedTypedAggregation {

    public static class Employee implements Serializable{

        private String name;

        private long salary;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSalary() {
            return salary;
        }

        public void setSalary(long salary) {
            this.salary = salary;
        }
    }

    public static class Average implements Serializable{

        private long sum;

        private long count;

        public Average() {
        }

        public Average(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

    public static class MyAverage extends Aggregator<Employee,Average,Double>{

        @Override
        public Average zero() {
            return new Average(0L,0L);
        }

        @Override
        public Average reduce(Average average, Employee employee) {
            long newSum = average.getSum() + employee.getSalary();
            long newCount = average.getCount() + 1;
            average.setSum(newSum);
            average.setCount(newCount);
            return average;
        }

        @Override
        public Average merge(Average b1, Average b2) {
            long mergeSum = b1.getSum() + b2.getSum();
            long mergeCount = b1.getCount() + b2.getCount();
            Average average = new Average();
            average.setSum(mergeSum);
            average.setCount(mergeCount);
            return average;
        }

        @Override
        public Double finish(Average reduction) {
            return ((double) reduction.getSum())/reduction.getCount();
        }

        @Override
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("user-defined-datasets-aggregation-example")
                .master("local[2]")
                .getOrCreate();

        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = "/Users/yqq/IdeaProjects/spark-demo/src/main/resources/employees.json";
        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
        ds.show();

        MyAverage myAverage = new MyAverage();
        TypedColumn<Employee, Double> average_salary = myAverage.toColumn().name("average_salary");
        Dataset<Double> res = ds.select(average_salary);
        res.show();

        spark.stop();



    }




}




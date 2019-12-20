package com.java.examples;

import java.io.Serializable;
import java.util.Random;

/**
 * 逻辑回归基于分类
 * Created by yqq on 2019/11/20.
 */
public class JavaHdfsLR {

    private static final int D = 10;
    private static final Random rand = new Random(42);

    static void showWarning() {
        String warning = "WARN: This is a naive implementation of Logistic Regression " +
                "and is given as an example!\n" +
                "Please use org.apache.spark.ml.classification.LogisticRegression " +
                "for more conventional use.";
        System.err.println(warning);
    }

    static class DataPoint implements Serializable{
        double[] x;
        double y;

        DataPoint(double[] x,double y){
            this.x = x;
            this.y = y;
        }
    }





    public static void main(String[] args) {

    }

}

package com.java8.test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntBinaryOperator;

/**
 * Created by yqq on 2019/6/26.
 *
 * lambda learn
 *
 * (parameters)-> expression or (parameters)->{statements;}
 */
public class Java8Tester {
    public static void main(String[] args) {

        /*List<String> nameList = new ArrayList<>();
        nameList.add("java8");
        nameList.add("hadoop");
        nameList.add("spark");
        nameList.forEach(System.out::println);*/


        Java8Tester tester = new Java8Tester();
        // def add
        MathOperation addition = (int a, int b) -> a + b;
        // def subtraction
        MathOperation subtraction = (a, b) -> a - b;
        // def multiplication
        MathOperation multiplication = (int a,int b) -> { return a*b; };
        // def division
        MathOperation division = (int a,int b) -> a/b;

        System.out.println("10+5="+tester.operate(10,5,addition));






    }

    private int operate(int a, int b, MathOperation mathOperation){

        return mathOperation.operation(a,b);
    }





}

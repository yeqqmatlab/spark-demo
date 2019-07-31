package com.java8.test;

import java.util.Random;

/**
 * Created by yqq on 2019/6/26.
 */
public class TestForEach {
    public static void main(String[] args) {
        Random random = new Random();
        //random.ints().limit(10).forEach(System.out::println);
        random.ints().limit(10).sorted().forEach(System.out::println);
    }
}

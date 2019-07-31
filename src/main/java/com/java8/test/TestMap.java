package com.java8.test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by yqq on 2019/6/26.
 */
public class TestMap {
    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(3, 4, 5, 6, 7, 2, 2, 3);
        List<Integer> collect = list.stream().map(i -> i * i).distinct().collect(Collectors.toList());
        collect.forEach(System.out::println);
    }
}

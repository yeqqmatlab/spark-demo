package com.java8.test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by yqq on 2019/6/26.
 */
public class TestStream {
    public static void main(String[] args) {
        List<String> stringsList = Arrays.asList("adc", "", "bc", "", " ", "ddd");

        List<String> collect = stringsList.stream()
                                .filter(str -> !str.trim().isEmpty())
                                .collect(Collectors.toList());
        System.out.println(collect.size());
        collect.forEach(System.out::println);

    }
}

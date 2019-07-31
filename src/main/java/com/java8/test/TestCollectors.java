package com.java8.test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by yqq on 2019/6/26.
 */
public class TestCollectors {
    public static void main(String[] args) {
        List<String> stringList = Arrays.asList("abc", "sss", "sss", "", "www", "", "", "ddd");
        String mergedString = stringList.stream().filter(string -> !string.isEmpty()).collect(Collectors.joining(","));
        System.out.println(mergedString);
    }
}

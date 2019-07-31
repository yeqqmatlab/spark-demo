package com.java8.test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by yqq on 2019/6/26.
 * 并行程序
 */
public class Testparallel {
    public static void main(String[] args) {
        List<String> stringList = Arrays.asList("abc", "sss", "sss", "", "www", "", "", "ddd");
        long count = stringList.parallelStream().filter(string -> string.isEmpty()).count();
        System.out.println(count);
    }
}

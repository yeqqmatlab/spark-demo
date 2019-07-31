package com.java8.test;

import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;

/**
 * Created by yqq on 2019/6/26.
 */
public class TestIntSummaryStatistics {
    public static void main(String[] args) {
        List<Integer> numbers = Arrays.asList(3, 2, 1, 22, 2, 2, 3, 7, 3, 5, 45);
        IntSummaryStatistics statistics = numbers.stream().mapToInt(x -> x).summaryStatistics();
        int min = statistics.getMin();
        int max = statistics.getMax();
        double average = statistics.getAverage();
        long sum = statistics.getSum();
        long count = statistics.getCount();
        System.out.println(min+"--"+max+"--"+average+"--"+sum+"--"+count);


    }
}

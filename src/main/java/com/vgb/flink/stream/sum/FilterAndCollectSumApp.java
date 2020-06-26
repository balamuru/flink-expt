package com.vgb.flink.stream.sum;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;

public class FilterAndCollectSumApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env
                = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> amounts = env.fromElements(1, 29, 40, 50);

        final int threshold = 30;
        final List<Integer> collect = amounts
                .filter(a -> a > threshold)
                .reduce((Integer a1, Integer b) -> Integer.sum(a1, b))
                .collect();

        collect.forEach(integer -> System.err.println("### " + integer));
    }
}

package com.xyk.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author mloine
 * @Description 离线处理 worldcount
 * @Date 10:05 上午 2021/8/30
 */
public class worldCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "/Users/xueyongkang/gitworkspace/flink-helloworld/src/main/resources/word.text";

        DataSource<String> dataSet = executionEnvironment.readTextFile(filePath);

        AggregateOperator<Tuple2<String, Integer>> sum = dataSet
                //带泛型的二元组 编译的时候会泛型擦除。推断不出的情况（不能直接勇lamba表达式）
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] s1 = s.split(" ");
                        for (String x : s1) {
                            collector.collect(new Tuple2(x, 1));
                        }

                    }
                }).groupBy(0)
                .sum(1);

        sum.print();

    }
}

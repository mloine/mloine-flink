package com.xyk.wc.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author mloine
 * @Description transform base操作 基本转换操作
 * map,flatMap,filter
 * @Date 10:32 下午 2021/9/4
 */
public class TransformTest1_Base {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.source
        DataStreamSource<String> dataSource = env.readTextFile("/Users/xueyongkang/gitworkspace/flink-helloworld/src/main/resources/sensor.txt");

        //2.transfer

        //map 把string转换成长度输出
        SingleOutputStreamOperator<Integer> mapStream = dataSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        //flatmap 按逗号分词
        SingleOutputStreamOperator<String> flatMapStream = dataSource.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {

                String[] fields = value.split(",");
                for (String field : fields) {
                    out.collect(field);

                }
            }
        });

        //filter: 删选snesor_1的数据
        SingleOutputStreamOperator<String> filterStream = dataSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("snesor_1");
            }
        });

        //3.slink 打印输出
        mapStream.print("mapStream");
        flatMapStream.print("flatMapStream");
        filterStream.print("filterStream");

        env.execute();

    }
}

package com.xyk.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author mloine
 * @Description  流式处理
 * com.xyk.wc.StreamWorldCount
 * @Date 11:36 下午 2021/8/30
 */
public class StreamWorldCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        executionEnvironment.setParallelism(4);
//        executionEnvironment.disableOperatorChaining();

        //从文件里面读取数据
//        String filePath = "/Users/xueyongkang/gitworkspace/flink-helloworld/src/main/resources/word.text";
//        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile(filePath);

        //用 paramter tool工具从程序启动参数中 提取配置项目
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        /**
         * source
         */
        //从socket文本流读取数据
        DataStream<String> stringDataStreamSource = executionEnvironment.socketTextStream(host,port);

        /**
         * Transformation
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");

                for (String x : s1) {
                    collector.collect(new Tuple2<>(x, 1));
                }
            }
        }).keyBy(0)
                .sum(1).setParallelism(2);
//                .startNewChain();
//                .disableChaining();

        /**
         * SINK
         */
        sum.print().setParallelism(2);

        executionEnvironment.execute();
    }

    
    
}

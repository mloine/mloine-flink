package com.xyk.wc.apitest.transform;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author mloine
 * @Description
 * @Date 11:31 下午 2021/9/4
 */
public class TransformTest2_RollingAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.source
        DataStreamSource<String> dataSource = env.readTextFile("/Users/xueyongkang/gitworkspace/flink-helloworld/src/main/resources/sensor.txt");

//        SingleOutputStreamOperator<SensorReading> mapStream = dataSource.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String value) throws Exception {
//                String[] fields = value.split(",");
//                return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
//            }
//        });

        //2.transform
        SingleOutputStreamOperator<SensorReading> mapStream = dataSource.map((value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        }));

        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = mapStream.keyBy("id");

//        KeyedStream<SensorReading, String> keyedStream1 = mapStream.keyBy(SensorReading::getId);

        //滚动聚合 取当前最大的温度值
//        SingleOutputStreamOperator<SensorReading> resourceStream = keyedStream.max("temperature");
        SingleOutputStreamOperator<SensorReading> resourceStream = keyedStream.maxBy("temperature");

        //3.slink
        resourceStream.print();

        env.execute();
    }
}

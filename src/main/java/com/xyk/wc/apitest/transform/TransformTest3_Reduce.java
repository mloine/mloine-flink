package com.xyk.wc.apitest.transform;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author mloine
 * @Description 一般性聚合操作 reduce
 * @Date 11:31 下午 2021/9/4
 */
public class TransformTest3_Reduce {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //1.source
        DataStreamSource<String> dataSource = env.readTextFile("/Users/xueyongkang/gitworkspace/flink-helloworld/src/main/resources/sensor.txt");

        //2.transform
        SingleOutputStreamOperator<SensorReading> mapStream = dataSource.map((value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        }));

        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = mapStream.keyBy("id");

        //reduce滚动聚合 取当前最大的温度值以及时间戳
        SingleOutputStreamOperator<SensorReading> reduce = keyedStream.reduce((value1, value2) -> {
            return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
        });

        //3.slink
        reduce.print();

        env.execute();
    }
}

package com.xyk.wc.apitest.transform;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author mloine
 * @Description
 * @Date 11:32 下午 2021/9/5
 */
public class TransformTest6_Partition {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //1.source
        DataStreamSource<String> dataSource = env.readTextFile("/Users/xueyongkang/gitworkspace/flink-helloworld/src/main/resources/sensor.txt");

        //2.transform
        SingleOutputStreamOperator<SensorReading> mapStream = dataSource.map((value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        }));

        //重分区
        DataStream<String> shuffle = dataSource.shuffle();

        //2.transform


        //3.slink
//        dataSource.print("input");
//        shuffle.print("shuffle");
//        mapStream.print("mapStream");
//        mapStream.keyBy("id").print("keyBy");
        /**
         * 全部丢到下游的第一个分区 除非有强烈需求 否则不推荐
         */
        mapStream.global().print("global");

        env.execute();
    }


}

package com.xyk.wc.apitest.transform;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * @Author mloine
 * @Description
 * @Date 11:32 下午 2021/9/5
 */
public class TransformTest5_RichFunction {


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

        SingleOutputStreamOperator<Tuple2<String, Integer>> reusultStream = mapStream.map(new MyRichMap());

        reusultStream.print();

        env.execute();
    }


    public static class MyMap implements MapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }

    /**
     * h现富函数
     */
    public static class MyRichMap extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化工作 一般是定义状态 或者 和外部数据库连接,避免重复连接
            System.out.println("open");
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            // 关闭数据库连接,清空状态
            System.out.println("close");
            super.close();
        }
    }

}

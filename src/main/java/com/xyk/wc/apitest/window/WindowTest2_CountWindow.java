package com.xyk.wc.apitest.window;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author mloine
 * @Description nc
 * @Date 11:08 下午 2021/11/28
 */
public class WindowTest2_CountWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //处理时间语意
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //事件时间语意
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1.source
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //2.transform
        SingleOutputStreamOperator<SensorReading> mapStream = inputStream.map((value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        }));

        // 开计数窗口测试
        SingleOutputStreamOperator<Double> avgTempResultStream = mapStream.keyBy("id")
                // 按照步长输出
                .countWindow(10, 2).aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {

                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0.0, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {

                        return new Tuple2<>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                    }
                });

        avgTempResultStream.print();

        env.execute();
    }
}

package com.xyk.wc.apitest.window;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.yaml.snakeyaml.events.Event;

import java.time.Duration;

/**
 * @Author mloine
 * @Description
 * @Date 12:20 上午 2021/9/18
 */
public class WindowTest1_TimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //1.source
//        DataStreamSource<String> dataSource = env.readTextFile("/Users/xueyongkang/gitworkspace/flink-helloworld/src/main/resources/sensor.txt");
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //2.transform
        SingleOutputStreamOperator<SensorReading> mapStream = inputStream.map((value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        }));


        //3.kai窗口测试
        SingleOutputStreamOperator<Integer> resultStream = mapStream
                .keyBy("id")
                //滚动  tumbling && 滑动 sliding
//                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .timeWindow(Time.seconds(15))
                //回话 session
//                    .window(EventTimeSessionWindows.withGap(Time.seconds(60)))
                //全局 galob
//                    .countWindow(5)
                //窗口函数
                //增量
//                .reduce(new ReduceFunction<SensorReading>() {
//                    @Override
//                    public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
//                        return null;
//                    }
//                })
                //增量聚合函数
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer integer) {
                        return integer + 1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                });

        //全窗口函数
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream2 = mapStream.keyBy("id")
                .timeWindow(Time.seconds(15))
//                .process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
//
//
//                })
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        Long weindowEnd = window.getEnd();
                        Integer cont = IteratorUtils.toList(input.iterator()).size();
                        Tuple3<String, Long, Integer> stringLongIntegerTuple3 = new Tuple3<>(id, weindowEnd, cont);
                        out.collect(stringLongIntegerTuple3);
                    }
                });

        //

        //其他可选的api
        String tag = "late";
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>(tag) {

        };

        SingleOutputStreamOperator<SensorReading> sumStream = mapStream.keyBy("id")
                .timeWindow(Time.seconds(15))
//                        .trigger()
//                           .evictor()
                //延迟处理
                .allowedLateness(Time.minutes(1))
                //展示窗口关闭 但会丢到一个侧输出流里面
                .sideOutputLateData(outputTag)
                .sum("temperature");

        sumStream.getSideOutput(outputTag).print(tag);


//        resultStream.print();
        resultStream2.print();
        env.execute();
    }
}

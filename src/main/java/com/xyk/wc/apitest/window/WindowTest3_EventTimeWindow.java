package com.xyk.wc.apitest.window;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @Author mloine
 * @Description watermark 一种特殊的数据记录 单调递增
 * @Date 11:42 下午 2021/11/30
 */
public class WindowTest3_EventTimeWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);


        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7779);

        // transform 已经分配了时间戳和watermark
        SingleOutputStreamOperator<SensorReading> mapStream = inputStream.map((value -> {
                    String[] fields = value.split(",");
                    return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
                }))
                //生序数据设置时间时间和watermark
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
                    @Override
                    public long extractAscendingTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                })
                //乱序数据设置时间和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //需要明确指定泛型
        OutputTag<SensorReading> tag = new OutputTag<SensorReading>("last"){};
        //基于时间时间的开窗 聚合,统计15s内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = mapStream.keyBy("id")
                //开窗
                .timeWindow(Time.seconds(15))
                //延迟关闭的时间
                .allowedLateness(Time.minutes(1))
                //侧输出柳
                .sideOutputLateData(tag)
                .minBy("temperature");


        minTempStream.print("minTemp");
        minTempStream.getSideOutput(tag).print("later");

        env.execute();

    }
}

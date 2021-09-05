package com.xyk.wc.apitest.transform;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author mloine
 * @Description split被弃用了，分流 使用底层process实现
 * connect 实现合流
 * union 合流（数据类型必须一致）
 * @Date 11:31 下午 2021/9/4
 */
public class TransformTest4_MultipleStreams {


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

        //一.分流操作 按照温度值30度 为界分为两条流
        OutputTag<SensorReading> lowTag = new OutputTag<SensorReading>("low") {
        };
        OutputTag<SensorReading> highTag = new OutputTag<SensorReading>("high") {
        };

        SingleOutputStreamOperator<SensorReading> process = mapStream.process(new ProcessFunction<SensorReading, SensorReading>() {

            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                out.collect(value);

                if (value.getTemperature() > 30) {
                    ctx.output(highTag, value);
                } else {
                    //额外产生另外一个流
                    ctx.output(lowTag, value);
                }
            }

        });

        DataStream<SensorReading> lowOutput = process.getSideOutput(lowTag);
        DataStream<SensorReading> highOutput = process.getSideOutput(highTag);


        //3.slink
        mapStream.print("all");
        lowOutput.print("low");
        highOutput.print("high");


        //二.合并流 connect 将高温流转换成二元组类型 与低温流连接合并之后 输出状态信息
        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream = highOutput.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {

            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowOutput);
        SingleOutputStreamOperator<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2(value.getId(), "normal");
            }
        });

        resultStream.print("resultStream");


        //三.union 合并 （有要求：两个流数据类型必须一直 相比connect不够灵活）
        DataStream<SensorReading> union = highOutput.union(lowOutput);
        union.print("union");

        env.execute();
    }
}

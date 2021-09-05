package com.xyk.wc.apitest.source;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author mloine
 * @Description 一般用于测试
 * @Date 12:43 上午 2021/9/4
 */
public class SourceTest1_Collection {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.source
        DataStreamSource<SensorReading> dataSource = env.fromCollection(Arrays.asList(
                new SensorReading("snesor_1", 1547717201L, 5.2),
                new SensorReading("snesor_6", 1547718202L, 15.4),
                new SensorReading("snesor_7", 1547718203L, 6.7),
                new SensorReading("snesor_10", 1547718206L, 38.4)
        ));

//        DataStreamSource<Integer> dataSource_2 = env.fromCollection(Arrays.asList(1, 2, 4, 67, 189));
        DataStreamSource<Integer> dataSource_2 = env.fromElements(1, 2, 4, 67, 189);


        //2.transfer

        //3.sink
        dataSource.print("dataSource1");
        dataSource_2.print("dataSource2");


        env.execute("mloine job");
    }
}

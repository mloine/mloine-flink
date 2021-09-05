package com.xyk.wc.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author mloine
 * @Description  一般用于测试
 * @Date 12:58 上午 2021/9/4
 */
public class SourceTest2_File {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        //1.source
        DataStreamSource<String> dataSource = env.readTextFile("/Users/xueyongkang/gitworkspace/flink-helloworld/src/main/resources/sensor.txt");

        //2.transfer
//        dataSource.ma

        //3.slink
        dataSource.print();

        env.execute();
    }
}

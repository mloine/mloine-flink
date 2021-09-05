package com.xyk.wc.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author mloine
 * @Description kafka读取数据
 * ./bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic sensor
 * @Date 1:04 上午 2021/9/4
 */
public class SourceTest3_kafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.source
        /**
         * kafka连接器 比较种重要
         */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

        //3.slink
        dataStream.print();

        env.execute();
    }
}

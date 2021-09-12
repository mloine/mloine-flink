package com.xyk.wc.apitest.sink;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * @Author mloine
 * @Description kafka connect
 *  kafka 的source
 *  kafka 的slink
 *  实现类似与etl的第一步
 * @Date 11:50 下午 2021/9/12
 */
public class SinkTest1_kafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //1.source
//        DataStreamSource<String> dataSource = env.readTextFile("/Users/xueyongkang/gitworkspace/flink-helloworld/src/main/resources/sensor.txt");

        //1.source
        /**
         * kafka连接器 比较种重要
         */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

        //2.transform
        SingleOutputStreamOperator<String> mapStream = dataStream.map((value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2])).toString();
        }));

        //3.sink
        mapStream.addSink(new FlinkKafkaProducer<String>("localhost:9092", "sinktest", new SimpleStringSchema()));

        env.execute();


    }


}

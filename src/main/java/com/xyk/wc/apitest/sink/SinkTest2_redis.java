package com.xyk.wc.apitest.sink;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


/**
 * @Author mloine
 * @Description redis
 * @Date 11:16 下午 2021/9/13
 */
public class SinkTest2_redis {

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

        //3.sink
        //定义jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .build();


        mapStream.addSink(new RedisSink<>(config, new RedisMapper<SensorReading>() {

            //1.定义保存数据到redis的命令 存成hash表 hset senor_temp is temperate
            @Override
            public RedisCommandDescription getCommandDescription() {
                //hset 表名
                return new RedisCommandDescription(RedisCommand.HSET, "senor_temp");
            }

            @Override
            public String getKeyFromData(SensorReading data) {
                return data.getId();
            }

            @Override
            public String getValueFromData(SensorReading data) {
                return data.getTemperature().toString();
            }
        }));

        env.execute();


    }


}

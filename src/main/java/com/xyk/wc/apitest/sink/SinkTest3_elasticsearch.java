package com.xyk.wc.apitest.sink;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


/**
 * @Author mloine
 * @Description es
 * @Date 11:16 下午 2021/9/13
 */
public class SinkTest3_elasticsearch {

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
        List<HttpHost> hosts = Arrays.asList(new HttpHost("127.0.0.1", 9200));

        ElasticsearchSink config = new ElasticsearchSink.Builder<SensorReading>(hosts, new ElasticsearchSinkFunction<SensorReading>() {
            @Override
            public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
                //详细的elasticsearch的插入操作

                //1.定义成写入的数据类型
                HashMap<String, String> dataSource = new HashMap<String, String>();
                dataSource.put("id",element.getId());
                dataSource.put("temp",element.getTemperature().toString());
                dataSource.put("timestamp",element.getTimestamp().toString());

                //2.创建请求向es发起写入命令

               IndexRequest indexRequest = Requests.indexRequest()
                        .index("senor")
                        .type("readingdata")
                        .source(dataSource);

                indexer.add(indexRequest);

            }
        }).build();


        mapStream.addSink(config);

        env.execute();


    }


}

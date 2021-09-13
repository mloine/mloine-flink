package com.xyk.wc.apitest.sink;

import com.xyk.wc.apitest.beans.SensorReading;
import com.xyk.wc.apitest.source.SourceTest4_UDF;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


/**
 * @Author mloine
 * @Description jdbc 自定义sink
 * @Date 11:16 下午 2021/9/13
 */
public class SinkTest4_jdbc {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

//        //1.source
//        DataStreamSource<String> dataSource = env.readTextFile("/Users/xueyongkang/gitworkspace/flink-helloworld/src/main/resources/sensor.txt");
//
//
//        //2.transform
//        SingleOutputStreamOperator<SensorReading> mapStream = dataSource.map((value -> {
//            String[] fields = value.split(",");
//            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
//        }));

        DataStreamSource<SensorReading> mapStream = env.addSource(new SourceTest4_UDF.MySourceFunction());

        //3.sink
        mapStream.addSink(new MyJdbcSink());

        env.execute();


    }


    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {

        private Connection connection = null;

        //预编译器
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
            insertStmt = connection.prepareStatement("insert into senor_temp(id,temp) values(?,?)");
            updateStmt = connection.prepareStatement("update senor_temp set temp = ? where id = ?");
            super.open(parameters);
        }


        //每来一条执行一条语句
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {

            //直接执行更新语句 如果没有更新成功 就 插入
            updateStmt.setDouble(1,value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();

            if(updateStmt.getUpdateCount() == 0){
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2,value.getTemperature());
                insertStmt.execute();
            }

        }


        @Override
        public void close() throws Exception {
            updateStmt.close();
            insertStmt.close();
            connection.close();
            super.close();
        }
    }


}

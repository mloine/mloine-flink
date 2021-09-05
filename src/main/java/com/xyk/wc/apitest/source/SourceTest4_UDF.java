package com.xyk.wc.apitest.source;

import com.xyk.wc.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;
import java.util.Set;

/**
 * @Author mloine
 * @Description 模拟输入流
 * @Date 12:07 下午 2021/9/4
 */
public class SourceTest4_UDF {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.source
        DataStreamSource<SensorReading> dateStream = env.addSource(new MySourceFunction());

        //2.transform

        //3.slink
        dateStream.print();

        env.execute();

    }

    /**
     * 实现自定义的sourceFunction
     */
    public static class MySourceFunction implements SourceFunction<SensorReading> {

        /**
         * 定义一个标志位置 用来控制数据的产生
         */
        private Boolean cancelFlag = Boolean.TRUE;

        private HashMap<String, Double> collects = new HashMap<String, Double>();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {

            //定义一个随机数 发生器
            Random random = new Random();

            //设置10个传感器的初始温度值
            for (int i = 1; i <= 10; i++) {
                collects.put(
                        "sensor_" + i,
                        Double.valueOf(60 + random.nextGaussian() * 20)
                );
            }

            while (cancelFlag) {
                // snesor_1,1547717201,5.2
                for (String key : collects.keySet()) {
                    Double v = collects.get(key) + random.nextGaussian();
                    collects.put(key, v);
                    ctx.collect(new SensorReading(key, System.currentTimeMillis(), v));

                }

                //控制输出速度
                Thread.sleep(1000L);

            }
        }

        @Override
        public void cancel() {
            cancelFlag = Boolean.FALSE;
        }
    }
}

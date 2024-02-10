package com.jack.api;

import com.jack.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.api
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-02  00:02
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class Api01SumMaxApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDataStreamSource = env.fromElements(
                new WaterSensor("1", 12L, 15),
                new WaterSensor("1", 12L, 10000),
                new WaterSensor("2", 12L, 1),
                new WaterSensor("2", 12L, 5),
                new WaterSensor("2", 12L, 1555),
                new WaterSensor("4", 12L, 66)
        );
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = sensorDataStreamSource.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
//        waterSensorStringKeyedStream.print();
        SingleOutputStreamOperator<WaterSensor> vc = waterSensorStringKeyedStream.max("vc");
        vc.print();

        env.execute();
    }
}

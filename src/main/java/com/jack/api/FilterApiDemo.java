package com.jack.api;

import com.jack.bean.WaterSensor;
import com.jack.functions.MyFilterFunctionImpl;
import com.jack.functions.MyMapFunctionImpl;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
public class FilterApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDataStreamSource = env.fromElements(
                new WaterSensor("1", 12L, 15),
                new WaterSensor("2", 12L, 1555),
                new WaterSensor("3", 12L, 1)
        );

        SingleOutputStreamOperator<WaterSensor> filter = sensorDataStreamSource.filter(new MyFilterFunctionImpl());
        filter.print();
        env.execute();
    }
}

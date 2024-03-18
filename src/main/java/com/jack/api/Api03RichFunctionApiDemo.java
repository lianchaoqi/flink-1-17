package com.jack.api;

import com.jack.bean.WaterSensor;
import com.jack.functions.MyReduceFunctionImpl;
import javassist.runtime.Inner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
public class Api03RichFunctionApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop101", 7777);
        SingleOutputStreamOperator<String> map = socketTextStream.map(new RichMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.printf("我进来了");
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public String map(String value) throws Exception {
                return value + "========================";
            }
        });
        map.print();
        env.execute();
    }
}

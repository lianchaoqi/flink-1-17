package com.jack.strameApi;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.strameApi
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-15  17:28
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class ConnectStreamApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<Integer> int1 = env.fromElements(1, 2, 3);
        DataStreamSource<String> int2 = env.fromElements("a", "b", "c");


        ConnectedStreams<Integer, String> connect = int1.connect(int2);
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });

        map.print();

        env.execute();
    }
}

package com.jack.strameApi;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.strameApi
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-15  17:28
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class UnionStreamApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<Integer> int1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> int2 = env.fromElements(77, 5, 3);


        DataStream<Integer> unioned = int1.union(int2);

        unioned.print();

        env.execute();
    }
}

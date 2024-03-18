package com.jack.partition;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.partition
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-14  18:06
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class ApiPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(2);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop101", 7777);


//        socketTextStream.partitionCustom(new MyPartitionImpl(), new KeySelector<String, String>() {
//            @Override
//            public String getKey(String value) throws Exception {
//                return value;
//            }
//        });
        DataStream<String> stringDataStream = socketTextStream.partitionCustom(new MyPartitionImpl(), s -> s);

        stringDataStream.print();

        env.execute();
    }
}

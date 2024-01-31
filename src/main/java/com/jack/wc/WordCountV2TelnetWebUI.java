package com.jack.wc;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.wc
 * @Author: lianchaoqi
 * @CreateTime: 2024-01-25  22:37
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class WordCountV2TelnetWebUI {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop101", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = socketTextStream.flatMap(
                        (String line, Collector<Tuple2<String, Integer>> out) -> {
                            String[] strings = line.split(" ");
                            for (String word : strings) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                ).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);
        outputStreamOperator.print();
        env.execute();
    }
}

package com.jack.wc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
public class WordCountV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textDS = env.readTextFile("input/word.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = textDS.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] strings = line.split(" ");

                        for (String word : strings) {
                            Tuple2<String, Integer> stringIntegerTuple2 = Tuple2.of(word, 1);
                            //TODO 采集数据
                            out.collect(stringIntegerTuple2);
                        }

                    }
                }
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = tuple2SingleOutputStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1);

        outputStreamOperator.print();
        env.execute();

    }
}

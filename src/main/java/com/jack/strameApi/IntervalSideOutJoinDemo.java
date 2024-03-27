package com.jack.strameApi;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.strameApi
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-15  17:28
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class IntervalSideOutJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

//        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> tuple1 = env.fromElements(
                Tuple2.of(1, 1),
                Tuple2.of(1, 2),
                Tuple2.of(2, 11)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple2<Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((k, ts) -> k.f1 * 1000L)
        );


        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> tuple2 = env.fromElements(
                Tuple3.of(1, 4, 1),
                Tuple3.of(1, 11, 2),
                Tuple3.of(2, 10, 3)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple3<Integer, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((event, ts) -> event.f1 * 1000L)
        );

        KeyedStream<Tuple2<Integer, Integer>, Integer> tuple2IntegerKeyedStream = tuple1.keyBy(t -> t.f0);
        KeyedStream<Tuple3<Integer, Integer, Integer>, Integer> tuple3IntegerKeyedStream = tuple2.keyBy(t -> t.f0);
        SingleOutputStreamOperator<String> process = tuple2IntegerKeyedStream
                .intervalJoin(tuple3IntegerKeyedStream)
                .between(Time.seconds(-2), Time.seconds(2))
                .process(
                        new ProcessJoinFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>, String>() {
                            @Override
                            public void processElement(Tuple2<Integer, Integer> left, Tuple3<Integer, Integer, Integer> right, ProcessJoinFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                                out.collect(left.f0 + "," + right.f0 + "," + right.f1 + "," + right.f2);
                            }
                        }
                );

        process.print();


        env.execute();
    }
}

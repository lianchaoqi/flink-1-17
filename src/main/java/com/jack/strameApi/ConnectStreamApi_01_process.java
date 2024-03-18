package com.jack.strameApi;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.strameApi
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-15  17:28
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class ConnectStreamApi_01_process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

//        env.setParallelism(1);
        DataStreamSource<Tuple2<Integer, String>> tuple1 = env.fromElements(
                Tuple2.of(1, "aa"),
                Tuple2.of(1, "ada"),
                Tuple2.of(2, "vv")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> tuple2 = env.fromElements(
                Tuple3.of(1, "aa", 1),
                Tuple3.of(1, "ada", 2),
                Tuple3.of(2, "vv", 3)
        );


        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> tupleConnectedStreams = tuple1
                .connect(tuple2)
                .keyBy(tp1 -> tp1.f0, tp2 -> tp2.f0);

        SingleOutputStreamOperator<String> processed = tupleConnectedStreams.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
            Map<Integer, List<Tuple2<Integer, String>>> map1 = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> map2 = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                System.out.print(id);
                if (!map1.containsKey(id)) {
                    List<Tuple2<Integer, String>> tuple2s = new ArrayList<>();
                    tuple2s.add(value);
                    map1.put(id, tuple2s);
                } else {
                    //存在  添加进去
                    map1.get(id).add(value);
                }
                //判断是否关联上  如果关联上  输出
                if (map2.containsKey(id)) {
                    List<Tuple3<Integer, String, Integer>> tuple3s = map2.get(id);
                    tuple3s.forEach(tup -> out.collect("s1:" + value + "<----->" + "s2:" + tup));
                }

            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                System.out.print(id);

                if (!map2.containsKey(id)) {
                    List<Tuple3<Integer, String, Integer>> tuple2s = new ArrayList<>();
                    tuple2s.add(value);
                    map2.put(id, tuple2s);
                } else {
                    //存在  添加进去
                    map2.get(id).add(value);
                }
                //判断是否关联上  如果关联上  输出
                if (map2.containsKey(id)) {
                    List<Tuple2<Integer, String>> tuple3s = map1.get(id);
                    tuple3s.forEach(tup -> out.collect("s2:" + value + "<----->" + "s1:" + tup));
                }
            }
        });

        processed.print();

        env.execute();
    }
}

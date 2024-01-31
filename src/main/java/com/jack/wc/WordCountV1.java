package com.jack.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.wc
 * @Author: lianchaoqi
 * @CreateTime: 2024-01-25  22:37
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class WordCountV1 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> textDS = env.readTextFile("input/word.txt");

        FlatMapOperator<String, Tuple2<String, Integer>> tuple2FlatMapOperator = textDS.flatMap(
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

        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = tuple2FlatMapOperator.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);

        sum.print();

//        env.execute("wc test");

    }
}

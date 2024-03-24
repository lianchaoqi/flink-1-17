package com.jack.window;

import com.jack.bean.WaterSensor;
import com.jack.functions.MySplitMapFunctionImpl;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.window
 * @Author: lianchaoqi
 * @CreateTime: 2024-03-18  22:16
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class AggregateDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop101", 7777);

        SingleOutputStreamOperator<WaterSensor> mapWater = socketTextStream.map(new MySplitMapFunctionImpl());

        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = mapWater.keyBy(WaterSensor::getId);

        WindowedStream<WaterSensor, String, TimeWindow> window = waterSensorStringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> aggregate = window.aggregate(
                new AggregateFunction<WaterSensor, Integer, String>() {
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            @Override
            public Integer add(WaterSensor value, Integer accumulator) {
                return accumulator + value.getVc();
            }

            @Override
            public String getResult(Integer accumulator) {
                return "最后温度：" + accumulator;
            }

            //只在会话窗口有效
            @Override
            public Integer merge(Integer a, Integer b) {
                return null;
            }
        });

        aggregate.print();

        env.execute();
    }
}

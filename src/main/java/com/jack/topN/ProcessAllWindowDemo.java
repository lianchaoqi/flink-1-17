package com.jack.topN;

import com.jack.bean.WaterSensor;
import com.jack.functions.MySplitMapFunctionImpl;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.window
 * @Author: lianchaoqi
 * @CreateTime: 2024-03-18  22:16
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class ProcessAllWindowDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop101", 7777);

        SingleOutputStreamOperator<WaterSensor> mapWater = socketTextStream.map(new MySplitMapFunctionImpl());


        //添加时间水位线  获取时间的时间水位线
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy =
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((wa, ts) -> wa.getTs() * 1000L);

        SingleOutputStreamOperator<String> process = mapWater.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                //全窗口函数只在窗口出发时执行一次
                .process(new MyProcessFunction());

        process.print();
        env.execute();
    }



    //全窗口函数只在窗口出发时执行一次
    public static class MyProcessFunction extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow> {
        @Override
        public void process(Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
            out.collect("当前窗口的数据：" + elements);
        }
    }
}

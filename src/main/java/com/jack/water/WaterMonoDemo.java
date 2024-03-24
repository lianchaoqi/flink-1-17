package com.jack.water;

import com.jack.bean.WaterSensor;
import com.jack.functions.MySplitMapFunctionImpl;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.window
 * @Author: lianchaoqi
 * @CreateTime: 2024-03-18  22:16
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class WaterMonoDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop101", 7777);

        SingleOutputStreamOperator<WaterSensor> mapWater = socketTextStream.map(new MySplitMapFunctionImpl());


        //添加时间水位线  获取时间的时间水位线
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long recordTimestamp) {
                System.out.println("我设置了随水位线 = " + waterSensor.getTs());
                return waterSensor.getTs() * 1000L;
            }
        });

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator =
                mapWater.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);

        WindowedStream<WaterSensor, String, TimeWindow> window =
                waterSensorStringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));


        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                TimeWindow window1 = context.window();
                long start = window1.getStart();
                long end = window1.getEnd();
                long l = elements.spliterator().estimateSize();
                out.collect(key + "|开始" + start + "|结束" + end + "数据" + l + "条" + elements.toString());
            }
        });

        process.print();
        env.execute();
    }
}

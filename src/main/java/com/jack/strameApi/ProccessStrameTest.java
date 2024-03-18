package com.jack.strameApi;

import com.jack.bean.WaterSensor;
import com.jack.functions.MySplitMapFunctionImpl;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.sidestrame
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-14  21:22
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class ProccessStrameTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop101", 7777);

        SingleOutputStreamOperator<WaterSensor> map = socketTextStream.map(new MySplitMapFunctionImpl());
        OutputTag<WaterSensor> s1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> process = map.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    //侧输出流s1
                    ctx.output(s1, value);
                } else if ("s2".equals(value.getId())) {
                    //侧输出流s1
                    ctx.output(s2, value);
                } else {
                    //主流
                    out.collect(value);
                }
            }
        });


        SideOutputDataStream<WaterSensor> s1pt = (SideOutputDataStream<WaterSensor>) process.getSideOutput(s1);
        SideOutputDataStream<WaterSensor> s2pt = (SideOutputDataStream<WaterSensor>) process.getSideOutput(s2);

        process.print("我是主流");
        s1pt.print("侧输出流s1");
        s2pt.print("侧输出流s2");
        env.execute();

    }
}

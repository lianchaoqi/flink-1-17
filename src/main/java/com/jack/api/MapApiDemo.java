package com.jack.api;

import com.jack.bean.WaterSensor;
import com.jack.functions.MyMapFunctionImpl;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.api
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-02  00:02
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class MapApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDataStreamSource = env.fromElements(
                new WaterSensor("1", 12L, 1331),
                new WaterSensor("2", 12L, 1331),
                new WaterSensor("3", 12L, 1331)
        );
        //方法一  匿名函数
//        SingleOutputStreamOperator<String> mapWater = sensorDataStreamSource.map(new MapFunction<WaterSensor, String>() {
//            @Override
//            public String map(WaterSensor waterSensor) throws Exception {
//                return waterSensor.getId();
//            }
//        });

        //方法二 匿名函数简写

//        SingleOutputStreamOperator<String> mapWater = sensorDataStreamSource.map(waterSensor -> waterSensor.getId());
//

        //方法三 公共function实现类
        SingleOutputStreamOperator<String> mapWater = sensorDataStreamSource.map(new MyMapFunctionImpl());

        mapWater.print();
        env.execute();
    }
}

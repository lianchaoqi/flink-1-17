package com.jack.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.source
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-01  23:22
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "num = " + aLong;
            }
        },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );
        DataStreamSource dataDEmo = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataDEmo");
        dataDEmo.print();
        env.execute();


    }
}

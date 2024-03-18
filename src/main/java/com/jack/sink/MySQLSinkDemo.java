package com.jack.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.jar.JarEntry;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.sink
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-21  23:28
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class MySQLSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "num = " + aLong;
            }
        },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );
        DataStreamSource dataDEmo = env.fromSource(dataGeneratorSource,
                WatermarkStrategy.noWatermarks(), "dataDEmo");

//        JdbcSink.sink(, )
//        FileSink.<String>forRowFormat(new Path("./tmp"),new SimpleStringEncoder<>("UTF-8"))
//        dataDEmo.sinkTo()
        env.execute();
    }
}

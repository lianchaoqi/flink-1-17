package com.jack.functions;

import com.jack.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.functions
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-02  00:09
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class MyMapFunctionImpl implements MapFunction<WaterSensor, String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}

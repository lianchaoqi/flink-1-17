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
public class MySplitMapFunctionImpl implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] strings = value.split(",");
        return new WaterSensor(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2]));
    }
}

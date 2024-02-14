package com.jack.functions;

import com.jack.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.functions
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-02  00:09
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class MyReduceFunctionImpl implements ReduceFunction<WaterSensor> {

    @Override
    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
        WaterSensor waterSensor = new WaterSensor();
        if (value1.getVc() <= value2.getVc()) {
            waterSensor.setId(value2.getId());
            waterSensor.setVc(value2.getVc());

        }else {
            waterSensor.setId(value1.getId());
            waterSensor.setVc(value1.getVc());
        }
        waterSensor.setTs(value2.getTs());
        return waterSensor;
    }
}

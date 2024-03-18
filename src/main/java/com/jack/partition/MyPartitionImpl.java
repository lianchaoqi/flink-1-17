package com.jack.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @BelongsProject: flink-1-17
 * @BelongsPackage: com.jack.partition
 * @Author: lianchaoqi
 * @CreateTime: 2024-02-14  17:52
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public class MyPartitionImpl implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}

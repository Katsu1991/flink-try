package com.gongyu.flink.stream.partition_strategy;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * 场景:增大分区、提高并行度，解决数据倾斜
 * DataStream → DataStream 分区元素随机均匀分发到下游分区，网络开销比较大
 *
 * 上游数据比较随意的分发到下游
 *
 * @author gongyu
 */
public class Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> stream = env.generateSequence(1, 10).setParallelism(1);

        stream.shuffle().print();

        env.execute();
    }
}

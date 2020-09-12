package com.gongyu.flink.stream.partition_strategy;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 上游分区数据分发到下游对应分区中
 * @author gongyu
 */
public class Forward {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Long> stream = env.generateSequence(1, 10);

        stream.forward().print();

        env.execute();
    }
}

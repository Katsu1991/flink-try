package com.gongyu.flink.stream.partition_strategy;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * 轮询分区元素，均匀的将元素分发到下游分区，下游每个分区的数据比较均匀，在发生数据倾斜时非常
 * 有用，网络开销比较大
 * @author gongyu
 */
public class Rebalance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> stream = env.generateSequence(1, 10).setParallelism(1);

        stream.rebalance().print();

        env.execute();
    }
}

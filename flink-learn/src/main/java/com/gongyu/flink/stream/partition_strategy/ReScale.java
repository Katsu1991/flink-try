package com.gongyu.flink.stream.partition_strategy;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 场景:减少分区 防止发生大量的网络传输 不会发生全量的重分区
 * DataStream → DataStream 通过轮询分区元素，将一个元素集合从上游分区发送给下游分区，发送单位是集合，而不是一个个元素
 * 注意:rescale发生的是本地数据传输，而不需要通过网络传输数据，比如taskmanager的槽数。简单 来说，上游的数据只会发送给本TaskManager中的下游
 * @author gongyu
 */
public class ReScale {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> stream = env.generateSequence(1, 10).setParallelism(1);

        stream.rescale().print();

        env.execute();
    }
}

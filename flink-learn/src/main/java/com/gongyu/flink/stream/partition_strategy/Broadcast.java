package com.gongyu.flink.stream.partition_strategy;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  广播到所有下游的算子
 *  "broadcasted to every parallel instance of the next operation"
 * @author gongyu
 */
public class Broadcast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Long> stream = env.generateSequence(1, 10).setParallelism(2);
        stream.writeAsText("./data/broadcast/stream1").setParallelism(2);
        stream.broadcast().writeAsText("./data/broadcast/stream2").setParallelism(4);

        env.execute();
    }
}

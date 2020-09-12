package com.gongyu.flink.stream.partition_strategy;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * "the output values all go to the first instance of the next processing operator"
 * @author gongyu
 */
public class Global {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Long> stream = env.generateSequence(1, 10).setParallelism(2);
        stream.writeAsText("./data/global/stream1").setParallelism(2);
        stream.global().writeAsText("./data/global/stream2").setParallelism(4);

        env.execute();
    }
}

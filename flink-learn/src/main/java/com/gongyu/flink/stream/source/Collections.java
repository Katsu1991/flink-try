package com.gongyu.flink.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author gongyu
 */
public class Collections {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4);
        streamSource.print();
        env.execute();
    }
}

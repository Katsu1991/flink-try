package com.gongyu.flink.stream.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author gongyu
 */
public class Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 8888);

        streamSource.filter(t -> t.equalsIgnoreCase("abc")).print();
        env.execute();
    }
}

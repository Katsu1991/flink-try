package com.gongyu.flink.stream.partition_strategy;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author gongyu
 */
public class KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> stream = env.generateSequence(1, 10).setParallelism(2);

        stream.writeAsText("./data/keyby/stream1").setParallelism(2);
        stream
                .map(t-> new Tuple2(t, 1))
                .returns(Types.TUPLE(Types.LONG, Types.INT))
                .keyBy(0).writeAsText("./data/keyby/stream2").setParallelism(2);

        env.execute();
    }
}

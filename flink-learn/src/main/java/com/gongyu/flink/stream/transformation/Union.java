package com.gongyu.flink.stream.transformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author gongyu
 */
public class Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        stream1.union(stream2)
                .flatMap((String line, Collector<String> collector) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        collector.collect(word);
                    }
                })
                .returns(Types.STRING)
                .map(t -> new Tuple2<String, Integer>(t, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .reduce((t1, t2) -> new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1))
                .print();

        env.execute();
    }
}

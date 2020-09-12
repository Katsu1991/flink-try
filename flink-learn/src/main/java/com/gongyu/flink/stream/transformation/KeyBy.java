package com.gongyu.flink.stream.transformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * word count
 *
 * @author gongyu
 */
public class KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("node04", 8888);

        stream
                .flatMap((String s, Collector<String> collector) -> {
                    for (String value : s.split("\\s")) {
                        collector.collect(value);
                    }
                })
                .returns(Types.STRING)
                .map(t -> Tuple2.of(t, 1))
                //如果要用Lambda表示是，Tuple2是泛型，那就得用returns指定类型。
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .reduce((Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) -> new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1))
                //sum和上述reduce同效果
//                .sum(1)
                .print();

        env.execute();
    }
}

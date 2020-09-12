package com.gongyu.flink.stream.transformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author gongyu
 */
public class Fold {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("localhost", 8888);

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
                .fold("结果：", (String current, Tuple2<String, Integer> t2) -> current+t2.f0+",")
                .print();
        env.execute();
    }
}

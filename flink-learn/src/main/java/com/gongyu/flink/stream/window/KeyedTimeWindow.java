package com.gongyu.flink.stream.window;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author gongyu
 */
public class KeyedTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.socketTextStream("localhost", 8888);
        stream
                .flatMap((String text, Collector<String> collector) -> {
                    String[] values = text.split("\\s");
                    for (String value : values) {
                        if (StringUtils.isNotBlank(value)) {
                            collector.collect(value);
                        }
                    }

                })
                               .returns(Types.STRING)
                .map(t -> new Tuple2(t, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)

                .timeWindow(Time.seconds(2))
                .sum(1)
                .print();

        env.execute();
    }
}

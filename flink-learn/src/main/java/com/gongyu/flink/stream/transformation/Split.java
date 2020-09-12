package com.gongyu.flink.stream.transformation;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 给定一组数字，奇偶数分开打印
 * @author gongyu
 */
public class Split {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> stream = env.generateSequence(1, 10);
        SplitStream<Long> splitStream = stream.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> output = new ArrayList<>();
                if (value % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        });

        DataStream<Long> even = splitStream.select("even");
        DataStream<Long> odd = splitStream.select("odd");
        DataStream<Long> all = splitStream.select("even", "odd");
        even.print();
        odd.print();
        all.print();

        env.execute();
    }
}

package com.gongyu.flink.stream.transformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * 使用flatmap去除重复单词
 * @author gongyu
 */
public class FlatMap {
    private static Set<String> set = new HashSet<>();
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream("node04", 8888);

        /**
         * 匿名类
         */
        /*streamSource
                .map(t -> t.split(" "))
                .flatMap(new FlatMapFunction<String[], Object>() {
                    @Override
                    public void flatMap(String[] value, Collector<Object> out) throws Exception {
                        Set<String> set = new HashSet<>();
                        for (String v : value) {
                            set.add(v);
                        }
                        out.collect(set);
                    }
                }).print();*/


        /**
         * lambda表达式
         */

        streamSource.flatMap((String s, Collector<String> collector) -> {
            for (String v : s.split("\\s")) {
                if (!set.contains(v)) {
                    collector.collect(v);
                    set.add(v);
                }
            }
        }).returns(Types.STRING).print();


        env.execute();

    }
}

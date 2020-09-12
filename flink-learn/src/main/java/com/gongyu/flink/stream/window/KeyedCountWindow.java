package com.gongyu.flink.stream.window;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author gongyu
 */
public class KeyedCountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.socketTextStream("node04", 8888);
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
                /**
                 * 滚动窗口
                    size代表的值的意思是：当某个key的数量 = size时 数据流才向后流
                    例如：当size=2时，打印结果永远不会打印出超过当2的wordcound，输入 a a a a 时，会输出 (a,2) (a,1)

                 * slide = size : 相当于tumbling（滚动）窗口
                 * slide < size : 每次计算都会包含前一次的重复数据
                 * slide > size : 先进入窗口的数据会被丢弃调，导致数据丢失
                 */
                .countWindow(3, 2)
                .sum(1)
                .print();

        env.execute();
    }
}

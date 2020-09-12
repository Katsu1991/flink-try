package com.gongyu.flink.stream.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *  自定义触发
 *
 * @author gongyu
 */
public class CustomTrigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream
                .flatMap((String line, Collector<String> collector) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        collector.collect(word);
                    }
                })
                .returns(Types.STRING)
                .map(word -> {
                    return new Tuple2<String, Integer>(word, 1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .timeWindow(Time.seconds(5))
                //当收到spark时，立即触发窗口计算
                //注意，此时只有spark这个window触发计算，其他单词的window会一直等待下去（只打印spark的统计结果）
                .trigger(new Trigger<Tuple2<String, Integer>, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        if ("spark".equals(element.f0)) {
                            return TriggerResult.FIRE_AND_PURGE;
                        }
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                    }
                })
                .sum(1)
                .print();

        env.execute();
    }
}

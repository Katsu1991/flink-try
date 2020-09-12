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
 * @author gongyu
 */
public class UnKeyedTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("localhost", 8888);

        stream
                .flatMap((String text, Collector<String> collector) -> {
                    String[] split = text.split("\\s");
                    for (String s : split) {
                        collector.collect(s);
                    }
                })
                .returns(Types.STRING)
                .map(t -> new Tuple2(t, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .timeWindowAll(Time.seconds(2))
                .trigger(new Trigger<Tuple2, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple2 element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
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

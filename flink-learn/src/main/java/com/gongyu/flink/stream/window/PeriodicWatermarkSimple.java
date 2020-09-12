package com.gongyu.flink.stream.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * @author gongyu
 */
public class PeriodicWatermarkSimple {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);
        int delaySeconds = 3;

        env.socketTextStream("localhost", 8888)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(delaySeconds)) {
                    @Override
                    public long extractTimestamp(String element) {
                        return Long.parseLong(element.split(" ")[0]);
                    }
                })
                .map(line -> {
                    String[] arr = line.split(" ");
                    return new Tuple2<String, Integer>(arr[1], 1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .timeWindow(Time.seconds(5))
                .reduce(
                        new ReduceFunction<Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2 reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
                                return new Tuple2(v1.f0, v1.f1 + v2.f1);
                            }
                        },
                        new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                                System.out.println("window: " + context.window().getStart() + " --- " + context.window().getEnd());
                                for (Tuple2<String, Integer> e : elements) {
                                    out.collect(e.f0 + " : " + e.f1);
                                }
                            }
                        }
                )
                .print();
        env.execute();
    }
}

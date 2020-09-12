package com.gongyu.flink.stream.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 *
 * window 窗口大小计算公式
 *
 * timestamp - （timestamp - offset + windowsize） % windowsize
 *
 * @author gongyu
 */
public class AllowLateness {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<String> stream = env.socketTextStream("node04", 8888);
        //定义侧输出流标签
        OutputTag<Tuple2<Long, String>> lateTag = new OutputTag<Tuple2<Long, String>>("late") {};
        SingleOutputStreamOperator<Tuple2<Long, String>> value = stream
                .map(t -> {
                    String[] split = t.split("\\s");
                    return new Tuple2<Long, String>(Long.parseLong(split[0]), split[1]);
                })
                .returns(Types.TUPLE(Types.LONG, Types.STRING))
                //设置watermark 的delay时间为2s
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Long, String>>(Time.seconds(2)) {
                    //指定eventtime字段
                    @Override
                    public long extractTimestamp(Tuple2<Long, String> element) {
                        return element.f0;
                    }
                })
                //设置窗口大小为5s
                .timeWindowAll(Time.seconds(5))
                //窗口触发后3s内，如果又出现这个窗口的数据，这个窗口的所有数据会被重新计算，相当于窗口保留3s
                .allowedLateness(Time.seconds(3))
                //如果数据迟到时间超过5s，则输出到侧输出流中
                .sideOutputLateData(lateTag)
                //处理的是主流的数据，不会处理侧输出流的数据
                .process(new ProcessAllWindowFunction<Tuple2<Long, String>, Tuple2<Long, String>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple2<Long, String>> elements, Collector<Tuple2<Long, String>> out) throws Exception {
                        System.out.println(context.window().getStart() + " --- " + context.window().getEnd());
                        elements.forEach(e -> out.collect(e));
                    }
                });
        value.print("main");
        value.getSideOutput(lateTag).print("late");

        env.execute();
    }
}

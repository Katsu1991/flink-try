package com.gongyu.flink.stream.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * 根据条件打水印
 * @author gongyu
 */
public class PunctuatedWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //输入参数格式：时间戳 发词口 单词
        DataStreamSource<String> socketStrema = env.socketTextStream("localhost", 8888);
        int delayMilSeconds = 3000;
        String delayMonitorId = "001";
        SingleOutputStreamOperator<String> stringOperator = socketStrema.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {
            long maxWatermark;

            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
                //只有001卡口来的数据才添加水位线
                String monitorId = lastElement.split(" ")[1];
                long watermarkTime = 0;
                if (delayMonitorId.equals(monitorId)) {
                    watermarkTime = maxWatermark - delayMilSeconds;
                } else {
                    watermarkTime = maxWatermark;
                }
                return new Watermark(watermarkTime);
            }

            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                long timestamp = Long.parseLong(element.split(" ")[0]);
                maxWatermark = Math.max(maxWatermark, timestamp);
                return timestamp;
            }
        });

        stringOperator
                .map(line -> {
                    String[] arr = line.split(" ");
                    return new Tuple2<String, Integer>(arr[2], 1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                .print();

        env.execute();
    }
}

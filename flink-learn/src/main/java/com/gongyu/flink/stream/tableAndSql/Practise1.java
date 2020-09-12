package com.gongyu.flink.stream.tableAndSql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 每5s统计最近10s各卡扣流量
 *
 * @author gongyu
 */
public class Practise1 {
    public static void main(String[] args) throws Exception {
        dataStreamApi();
    }

    //table api 使用
    public static void tableApi() throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(2);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        SingleOutputStreamOperator<Tuple2<String, Long>> socketStream = streamEnv.socketTextStream("node04", 8888)
                .map(t -> {
                    String[] parts = t.split(",");
                    Tuple2<String, Long> data = new Tuple2<String, Long>();
                    data.f0 = parts[0].trim().replace("'", "");
                    String datetime = parts[4].trim().replace("'", "");
                    data.f1 = timestamp2Long(datetime);
                    return data;
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element) {
                        return element.f1;
                    }
                });

        Table table = tableEnv.fromDataStream(socketStream, "monitorId, datetime.rowtime");
        Table result = table.window(Slide.over("10.second").every("5.second").on("datetime").as("win"))
                .groupBy("win, monitorId")
                .select("monitorId, count(monitorId), win.start, win.end");

        tableEnv.toRetractStream(result, Row.class).print();

        tableEnv.execute("tableApi job");

    }


    //dataStream 使用
    public static void dataStreamApi() throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = streamEnv.socketTextStream("node04", 8888);
        socketTextStream
                .map(t -> {
                    String[] parts = t.split(",");
                    Tuple3<String, Long, Long> data = new Tuple3<String, Long, Long>();
                    data.f0 = parts[0].trim().replace("'", "");
                    String datetime = parts[4].trim().replace("'", "");
                    data.f1 = timestamp2Long(datetime);
                    data.f2 = 1L;
                    return data;
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.LONG))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Long>>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Long> element) {
                        return element.f1;
                    }
                })
                .keyBy(t -> t.f0)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .process(new MyProcessWindowFunction())
                .print();
        streamEnv.execute();
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, Long>, Tuple4<String, Long, Long, Long>, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, Long, Long>> elements, Collector<Tuple4<String, Long, Long, Long>> out) throws Exception {
            AtomicInteger size = new AtomicInteger();
            elements.forEach(t -> {
                size.getAndIncrement();
            });
            Tuple4<String, Long, Long, Long> result = new Tuple4<>();
            result.f0 = key;
            result.f1 = size.longValue();
            result.f2 = context.window().getStart();
            result.f3 = context.window().getEnd();
            out.collect(result);

            System.out.println("当前水印：" + context.currentWatermark());
        }
    }

    private static long timestamp2Long(String datetime) {
        String pattern = "yyyy-MM-dd HH:mm:ss";
        return LocalDateTime
                .parse(datetime, DateTimeFormatter.ofPattern(pattern))
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    @Test
    public void test() {
        String datetime = "2020-07-27 10:10:10";
        System.out.println(timestamp2Long(datetime));
    }
}

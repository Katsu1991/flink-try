package com.gongyu.flink.stream.tableAndSql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 每隔5秒，统计每个基站中通话成功的数量,假设数据基于呼叫时间乱序。
 *
 * @author gongyu
 */
public class TestTableAPIWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        SingleOutputStreamOperator<StationLog> stream = streamEnv.socketTextStream("node04", 8888)
                .map(line -> {
                    String[] arr = line.split(",");
                    return new StationLog(arr[0].trim(), arr[1].trim(), arr[2].trim(), arr[3].trim(), Long.parseLong(arr[4].trim()), Long.parseLong(arr[5].trim()));
                })

                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<StationLog>(Time.seconds(2)) {

                    @Override
                    public long extractTimestamp(StationLog element) {
                        return element.callTime;
                    }
                });

        Table table = tableEnv.fromDataStream(stream, "sid, callOut, callId, callType, callTime.rowtime");

        Table result = table.filter("callType === 'success'")
                .window(Tumble.over("5.second").on("callTime").as("win"))
                .groupBy("win, sid")
                .select("sid, count(sid), win.start, win.end");


        //TypeInformation[] fieldTypes = {Types.STRING, Types.LONG, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP};
        //可将下面的Row.class 替换成：Types.TUPLE(fieldTypes)
        tableEnv.toRetractStream(result, Row.class)
                .filter(t -> t.f0 == true)
                .print();

        tableEnv.execute("TestTableAPIWindow");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class StationLog {
        private String sid;
        private String callOut;
        private String callId;
        private String callType;
        private Long callTime;
        private Long duration;
    }
}

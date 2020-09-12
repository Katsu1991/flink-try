package com.gongyu.flink.stream.tableAndSql;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author gongyu
 */
public class TestFlinkSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        SingleOutputStreamOperator<TestTableAPIWindow.StationLog> stream = streamEnv.socketTextStream("node04", 8888)
                .map(t -> {
                    String[] arr = t.split(",");
                    return new TestTableAPIWindow.StationLog(arr[0].trim(), arr[1].trim(), arr[2].trim(), arr[3].trim(), Long.parseLong(arr[4].trim()), Long.parseLong(arr[5].trim()));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TestTableAPIWindow.StationLog>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(TestTableAPIWindow.StationLog element) {
                        return element.getCallTime();
                    }
                });

        //定义表名为：t_station
        tableEnv.registerDataStream("t_station", stream, "sid, callOut, callId, callType, callTime.rowtime, duration");
        //1.统计每个基站成功通话的次数
        //Table result = tableEnv.sqlQuery("select sid, count(sid) from t_station where callType = 'success' group by sid");
        //每隔5秒，每个基站通话成功的通话时长总和
        //Table result = tableEnv.sqlQuery("  select sid,sum(duration),tumble_start(callTime,interval '5' second),tumble_end(callTime,interval '5' second) from t_station  where callType='success' " +
          //      "group by tumble(callTime,interval '5' second),sid");
        //3、每隔5秒，统计每个基站，最近10秒内，所有通话成功的通话时长总和。窗口长度是10s,滑动步长是5s
        Table result = tableEnv.sqlQuery("select sid,sum(duration),hop_start(callTime,interval '5' second,interval '10' second),hop_end(callTime,interval '5' second,interval '10' second) from t_station  where callType='success' " +
                "group by hop(callTime,interval '5' second,interval '10' second),sid");
        //hop(三个参数)：第一个参数：时间字段,第二个：滑动步长，第三个:窗口长度

        tableEnv.toRetractStream(result, Row.class)
                .filter(t -> t.f0 == true)
                .print();

        tableEnv.execute("job TestFlinkSQL");

    }
}

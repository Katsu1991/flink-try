package com.gongyu.flink_projects.traffic_monitor.warning;

import com.gongyu.flink_projects.traffic_monitor.pojo.RepetitionCarWarning;
import com.gongyu.flink_projects.traffic_monitor.pojo.TrafficInfo;
import com.gongyu.flink_projects.traffic_monitor.sink.MysqlSink;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 查找套牌车
 * 逻辑：10s内有两个相同的车牌号经过同一个卡口，或不通的卡口
 *
 * @author gongyu
 */
public class RepetitionCar {
    private static ValueStateDescriptor state = new ValueStateDescriptor("before", TrafficInfo.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<TrafficInfo> inputStream = streamEnv.socketTextStream("node04", 8888)
                .map(line -> {
                    String[] arr = line.split(",");
                    return new TrafficInfo(Long.parseLong(arr[0]), arr[1], arr[2], arr[3], Double.valueOf(arr[4]), arr[5], arr[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TrafficInfo>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(TrafficInfo element) {
                        return element.getActionTime();
                    }
                });

        inputStream.keyBy(t -> t.getCarNo())
                .process(new KeyedProcessFunction<String, TrafficInfo, RepetitionCarWarning>() {
                    @Override
                    public void processElement(TrafficInfo now, Context ctx, Collector<RepetitionCarWarning> out) throws Exception {
                        ValueState<TrafficInfo> state = getRuntimeContext().getState(RepetitionCar.state);
                        TrafficInfo before = state.value();

                        if (null != before && Math.abs(now.getActionTime() - before.getActionTime()) < 10000) {
                            RepetitionCarWarning warning = RepetitionCarWarning.builder()
                                    .carNo(now.getCarNo())
                                    .msg("涉嫌套牌")
                                    .actionTime(ctx.timerService().currentProcessingTime())
                                    .build();
                            if (now.getActionTime() > before.getActionTime()) {
                                warning.setFirstMonitor(before.getMonitorId());
                                warning.setFirstMonitorTime(before.getActionTime());
                                warning.setSecondMonitor(now.getMonitorId());
                                warning.setSecondMonitorTime(now.getActionTime());
                            } else {
                                warning.setFirstMonitor(now.getMonitorId());
                                warning.setFirstMonitorTime(now.getActionTime());
                                warning.setSecondMonitor(before.getMonitorId());
                                warning.setSecondMonitorTime(before.getActionTime());
                            }
                            out.collect(warning);
                        }

                        if (null == before) {
                            state.update(now);
                        }else if (now.getActionTime() > before.getActionTime()) {
                            state.update(now);
                        }
                    }
                })
                .addSink(new MysqlSink<RepetitionCarWarning>());

        streamEnv.execute();
    }
}

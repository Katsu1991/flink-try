package com.gongyu.flink_projects.traffic_monitor.warning;


import com.gongyu.flink_projects.traffic_monitor.pojo.ExceedSpeedInfo;
import com.gongyu.flink_projects.traffic_monitor.pojo.MonitorInfo;
import com.gongyu.flink_projects.traffic_monitor.pojo.TrafficInfo;
import com.gongyu.flink_projects.traffic_monitor.source.MysqlDataSource;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 2mins内，有3次以上（包含3次）的超速行为，定位危险驾驶
 * 超速：实际速度 > 卡扣限速*1.2
 * 使用cep变成
 * @author gongyu
 */
public class DangerousDriver {
    private static MapStateDescriptor<String, MonitorInfo> descriptor = new MapStateDescriptor("DangerousDriver", String.class, MonitorInfo.class);
    private static final Integer DEFAULT_LIMIT_SPEED = 60;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        String sql = "select area_id, road_id, monitor_id, limit_speed from monitor_info";
        BroadcastStream speedStream = streamEnv.addSource(new MysqlDataSource(sql, MonitorInfo.class)).returns(MonitorInfo.class).broadcast(descriptor);

        SingleOutputStreamOperator<TrafficInfo> carStream = streamEnv.socketTextStream("node04", 8888)
                .map(line -> {
                    String[] arr = line.split(",");
                    return new TrafficInfo(Long.parseLong(arr[0]), arr[1], arr[2], arr[3], Double.valueOf(arr[4]), arr[5], arr[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TrafficInfo>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(TrafficInfo element) {
                        return element.getActionTime();
                    }
                });

        SingleOutputStreamOperator<ExceedSpeedInfo> exceedStream = carStream.connect(speedStream)
                .process(new BroadcastProcessFunction<TrafficInfo, MonitorInfo, ExceedSpeedInfo>() {

                    @Override
                    public void processElement(TrafficInfo value, ReadOnlyContext ctx, Collector<ExceedSpeedInfo> out) throws Exception {
                        MonitorInfo monitorInfo = ctx.getBroadcastState(descriptor).get(value.getMonitorId());
                        int limitSpeed = null == monitorInfo ? DEFAULT_LIMIT_SPEED : monitorInfo.getLimitSpeed();
                        out.collect(new ExceedSpeedInfo(value.getCarNo(), value.getAreaId(), value.getRoadId(), value.getMonitorId(), value.getSpeed(), limitSpeed, value.getActionTime()));
                    }

                    @Override
                    public void processBroadcastElement(MonitorInfo value, Context ctx, Collector<ExceedSpeedInfo> out) throws Exception {
                        ctx.getBroadcastState(descriptor).put(value.getMonitorId(), value);
                    }
                });

        //CEP
        Pattern<ExceedSpeedInfo, ExceedSpeedInfo> pattern = Pattern.<ExceedSpeedInfo>begin("begin")
                .where(new IterativeCondition<ExceedSpeedInfo>() {
                    @Override
                    public boolean filter(ExceedSpeedInfo value, Context<ExceedSpeedInfo> ctx) throws Exception {
                        return value.getLimitSpeed() * 1.2 < value.getSpeed();
                    }
                })
                .timesOrMore(3)
                .greedy()
                //30s内超速3次及以上
                .within(Time.seconds(30));

        PatternStream<ExceedSpeedInfo> ps = CEP.pattern(exceedStream.keyBy(t -> t.getCarNo()), pattern);
        ps.select(map -> {
            List<ExceedSpeedInfo> list = map.get("begin");
            StringBuilder sb = new StringBuilder();
            for (ExceedSpeedInfo info : list) {
                sb.append(" --- monitor：" + info.getMonitorId())
                        .append(" speed: " + info.getSpeed());
            }
            return list.get(0).getCarNo() + "dangerous driver， 详细信息：" + sb.toString();
        }).print();

        streamEnv.execute("DangerousDriver");
    }
}

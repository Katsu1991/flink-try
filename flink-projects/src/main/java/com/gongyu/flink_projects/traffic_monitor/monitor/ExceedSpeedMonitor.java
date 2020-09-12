package com.gongyu.flink_projects.traffic_monitor.monitor;

import com.gongyu.flink_projects.traffic_monitor.pojo.ExceedSpeedInfo;
import com.gongyu.flink_projects.traffic_monitor.pojo.MonitorInfo;
import com.gongyu.flink_projects.traffic_monitor.pojo.TrafficInfo;
import com.gongyu.flink_projects.traffic_monitor.source.MysqlDataSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 实时超速监控,抓取超速车辆信息
 * <p>
 * stream1：实时从db中获取各路口基本限速信息并broadcast到下游各算子
 * stream2：从kafka中获取各车辆经过各路口到基本信息，并判断是否超速
 *
 * @author gongyu
 */
public class ExceedSpeedMonitor {

    private static String topic = "t_traffic";
    private static String name = "monitor_info";
    private static MapStateDescriptor state = new MapStateDescriptor<String, MonitorInfo>(name, String.class,MonitorInfo.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        String sql = "select area_id, road_id, monitor_id, limit_speed from monitor_info where limit_speed > 0";
        BroadcastStream broadcastStream = streamEnv.addSource(new MysqlDataSource(sql, MonitorInfo.class)).returns(MonitorInfo.class).broadcast(state);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        prop.setProperty("group.id", "speed_monitor_001");

        SingleOutputStreamOperator<TrafficInfo> carStream = streamEnv.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop))
                .map(line -> {
                    String[] arr = line.split(",");
                    return new TrafficInfo(Long.parseLong(arr[0]), arr[1], arr[2], arr[3], Double.valueOf(arr[4]), arr[5], arr[6]);
                });

        carStream.connect(broadcastStream).process(new BroadcastProcessFunction<TrafficInfo, MonitorInfo, ExceedSpeedInfo>() {

            @Override
            public void processElement(TrafficInfo value, ReadOnlyContext ctx, Collector<ExceedSpeedInfo> out) throws Exception {
                MonitorInfo monitorInfo = (MonitorInfo) ctx.getBroadcastState(state).get(value.getMonitorId());
                if (null != monitorInfo) {
                        if (monitorInfo.getLimitSpeed() * 1.1 < value.getSpeed()) {
                            ExceedSpeedInfo exceedSpeedInfo = ExceedSpeedInfo.builder()
                                    .carNo(value.getCarNo())
                                    .monitorId(value.getMonitorId())
                                    .areaId(value.getAreaId())
                                    .roadId(value.getRoadId())
                                    .speed(value.getSpeed())
                                    .limitSpeed(monitorInfo.getLimitSpeed())
                                    .actionTime(value.getActionTime())
                                    .build();
                            out.collect(exceedSpeedInfo);
                        }
                }
            }

            @Override
            public void processBroadcastElement(MonitorInfo value, Context ctx, Collector<ExceedSpeedInfo> out) throws Exception {
                ctx.getBroadcastState(state).put(value.getMonitorId(), value);
            }
        }).print();


        streamEnv.execute();
    }
}

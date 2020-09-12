package com.gongyu.flink_projects.traffic_monitor.warning;

import com.gongyu.flink_projects.traffic_monitor.pojo.TrafficInfo;
import com.gongyu.flink_projects.traffic_monitor.pojo.ViolationCarInfo;
import com.gongyu.flink_projects.traffic_monitor.source.MysqlDataSource;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 将违法车辆运行轨迹存入hbase
 * 是否是违法车辆的依据源自违法车辆信息表
 *
 * @author gongyu
 */
public class CarTraceInfo {
    private static MapStateDescriptor vialationListState = new MapStateDescriptor("ViolationCarInfo", String.class, ViolationCarInfo.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String sql = "select car_no, msg, create_time from violation_car_info";
        BroadcastStream violationStream = streamEnv.addSource(new MysqlDataSource(sql, ViolationCarInfo.class)).returns(ViolationCarInfo.class).broadcast(vialationListState);

        SingleOutputStreamOperator<TrafficInfo> carStream = streamEnv.socketTextStream("node04", 8888)
                .map(line -> {
                    String[] arr = line.split(",");
                    return new TrafficInfo(Long.parseLong(arr[0]), arr[1], arr[2], arr[3], Double.valueOf(arr[4]), arr[5], arr[6]);
                });
        carStream.connect(violationStream)
                .process(new BroadcastProcessFunction<TrafficInfo, ViolationCarInfo, TrafficInfo>() {

                    @Override
                    public void processElement(TrafficInfo value, ReadOnlyContext ctx, Collector<TrafficInfo> out) throws Exception {
                        System.out.println("新来一条car info ：" + value.getCarNo());
                        ViolationCarInfo object = (ViolationCarInfo) ctx.getBroadcastState(vialationListState).get(value.getCarNo());
                        if (null != object) {
                            out.collect(value);
                        }
                    }

                    @Override
                    public void processBroadcastElement(ViolationCarInfo value, Context ctx, Collector<TrafficInfo> out) throws Exception {
                        System.out.println("新来一条violation info ：" + value.getCarNo());
                        ctx.getBroadcastState(vialationListState).put(value.getCarNo(), value);
                    }
                })
                .print();

        streamEnv.execute("CarTraceInfo");
    }
}

package com.gongyu.flink.stream.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 统计每辆车的运行轨迹
 *
 * @author gongyu
 */
public class ListStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);
        //input 车辆 卡口号
        socketStream
                .map(line -> {
                    String[] arr = line.split(" ");
                    return new Tuple2<String, String>(arr[0], arr[1]);
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, String>, String>() {
                    ListState<String> trackStates = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("trackStates", String.class);
                        //可以通过如下方式设置statue生命周期
                        StateTtlConfig stateTtlConfig = StateTtlConfig
                                //指定ttl时长为10s
                                .newBuilder(Time.seconds(10))
                                //指定ttl刷新时只对创建和写入操作有效
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                //指定状态可见性为永远不返回过期数据
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        descriptor.enableTimeToLive(stateTtlConfig);

                        trackStates = getRuntimeContext().getListState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                        trackStates.add(value.f1);

                        String traceInfo = getTraceString(trackStates.get());
                        out.collect(value.f0 + " trace: " + traceInfo);
                    }
                }).uid("process")
                .print();
        env.execute();
    }

    private static String getTraceString(Iterable<String> MonitorIdList) {
        StringBuilder sb = new StringBuilder();
        for (Object monitorId : MonitorIdList) {
            sb.append(monitorId);
            sb.append("--->");
        }
        String str = sb.toString();
        return str.substring(0, str.length() - 4);
    }
}

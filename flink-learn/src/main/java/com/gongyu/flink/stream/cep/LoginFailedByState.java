package com.gongyu.flink.stream.cep;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author gongyu
 */
public class LoginFailedByState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<LoginFailedByCEP.LoginEvent> stream = env
                .fromCollection(LoginFailedByCEP.getDataSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginFailedByCEP.LoginEvent>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(LoginFailedByCEP.LoginEvent element) {
                        return element.getLoginTime();
                    }
                });

        stream
                .keyBy(t -> t.getUsername())
                .process(new KeyedProcessFunction<String, LoginFailedByCEP.LoginEvent, String>() {
                    MapState<LoginFailedByCEP.LoginEvent, Integer> mapState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor descriptor = new MapStateDescriptor("descriptor", LoginFailedByCEP.LoginEvent.class, Integer.class);
                        mapState = getRuntimeContext().getMapState(descriptor);
                    }

                    @Override
                    public void processElement(LoginFailedByCEP.LoginEvent value, Context ctx, Collector<String> out) throws Exception {
                        Integer failedTimes = mapState.get(value);
                        if ("fail".equalsIgnoreCase(value.getLoginType())) {
                            if (null == failedTimes) {
                                mapState.put(value, 1);
                                return;
                            }
                            if (failedTimes.intValue() >= 2) {
                                out.collect(value.getUsername() + "login failed times >= 3");
                                mapState.clear();
                            } else {
                                mapState.put(value, failedTimes.intValue() + 1);
                            }
                        }
                    }
                })
                .print();

        env.execute();
    }
}

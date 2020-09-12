package com.gongyu.flink.stream.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 前后2次速度之差超过50，则超速报警
 *
 * @author gongyu
 */
public class ValueStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 8888);

        socketStream
                .map(line -> {
                    String[] arr = line.split(" ");
                    return new Tuple2<String, String>(arr[0], arr[1]);
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, String>, String>() {
                    ValueState<String> speedState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("descriptor", String.class);
                        speedState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                        if (null != speedState.value()) {
                            double lastSpeed = Double.valueOf(speedState.value());
                            double nowSpeed = Double.valueOf(value.f1);
                            if (nowSpeed - lastSpeed > 50) {
                                out.collect(value.f0 + " overspeed...");
                            }
                        }
                        speedState.update(value.f1);
                    }
                }).print();
        env.execute();
    }
}

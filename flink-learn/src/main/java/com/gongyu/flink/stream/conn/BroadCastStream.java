package com.gongyu.flink.stream.conn;

import com.gongyu.flink.stream.kafka.KafkaConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 往kafka中写id-城市映射数据
 * 往socket中写城市id
 * <p>
 * 通过connect 实时通过最新的映射表找到对应的城市名称
 *
 * @author gongyu
 */
public class BroadCastStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = KafkaConfig.getProducerProperties();
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("config", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromLatest();

        //配置流
        DataStreamSource<String> confStream = env.addSource(kafkaConsumer);
        //业务流
        DataStreamSource<String> socketStream = env.socketTextStream("node04", 8888);
        //定义map state描述器


        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("dynamicConfig",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        //设置广播流的数据描述信息
        BroadcastStream<String> broadcastStream = confStream.broadcast(descriptor);
        //connect关联业务流与配置信息流，broadcastStream流中的数据会广播到下游的各个线程中
        socketStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    //每来一个新的元素都会调用一下这个方法
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                        String city = broadcastState.get(value);
                        if (null == city) {
                            out.collect("not found");
                        } else {
                            out.collect(city);
                        }
                    }

                    //kafka中配置流信息，写入到广播流中
                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                        //kafka中的数据
                        String[] values = value.split("\\s");
                        broadcastState.put(values[0], values[1]);
                    }
                }).print();

        env.execute();
    }
}

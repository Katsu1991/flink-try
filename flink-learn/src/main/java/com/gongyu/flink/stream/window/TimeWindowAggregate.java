package com.gongyu.flink.stream.window;

import com.gongyu.flink.stream.kafka.KafkaConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 使用增量聚合函数统计最近20s内，各个卡口的车流量
 *
 * @author gongyu
 */
public class TimeWindowAggregate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = KafkaConfig.getProducerProperties();
        String topic = "flink-kafka";

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                props);

        //数据流：310999005201	苏EJ789X	2014-08-20 14:09:43	255
        DataStreamSource<String> stream = env.addSource(flinkKafkaConsumer
        );
        stream
                .map(t -> new Tuple2<String, Integer>(t.split("\\s")[0], 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .timeWindow(Time.seconds(20))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {

                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return new Tuple2<>(null, 0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
                        return new Tuple2<>(value.f0, value.f1 + accumulator.f1);
                    }

                    @Override
                    public String getResult(Tuple2<String, Integer> accumulator) {
                        return "车流量：" + accumulator.f0 + " " + accumulator.f1;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return new Tuple2<>(a.f0, a.f1 + b.f1);
                    }
                }).print();

        env.execute();
    }
}

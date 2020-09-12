package com.gongyu.flink.stream.sink;

import com.gongyu.flink.stream.kafka.KafkaConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * word count 写入kafka
 *
 * @author gongyu
 */
public class KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("node04", 8888);
        SingleOutputStreamOperator<Tuple2> result = stream
                .flatMap((String text, Collector<String> collector) -> {
                    for (String s : text.split("\\s")) {
                        if (StringUtils.isNotBlank(s)) {
                            collector.collect(s);
                        }
                    }

                })
                .returns(Types.STRING)
                .map(t -> new Tuple2(t, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1);


        Properties properties = KafkaConfig.getProducerProperties();

        result.addSink(new FlinkKafkaProducer<Tuple2>(
                "wc",
                new KafkaSerializationSchema<Tuple2>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2 tuple2, @Nullable Long aLong) {
                        return new ProducerRecord<>("wc", String.valueOf(tuple2.f0).getBytes(), String.valueOf(tuple2.f1).getBytes());
                    }
                },
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        ));

        env.execute();
    }
}

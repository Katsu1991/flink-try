package com.gongyu.flink.stream.source;

import com.gongyu.flink.stream.kafka.KafkaConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author gongyu
 */
public class KafkaNoKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = KafkaConfig.getProducerProperties();
        String topic = "flink-kafka";
        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties));

        streamSource.print();
        env.execute();
    }
}

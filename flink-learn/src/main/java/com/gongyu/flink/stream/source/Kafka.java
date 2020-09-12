package com.gongyu.flink.stream.source;

import com.gongyu.flink.stream.kafka.KafkaConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * 命令行启动有key发送窗口
 * kafka-console-producer.sh  --broker-list node01:9092,node02:9092,node03:9092 --topic flink-kafka  --property parse.key=true
 * 默认消息键与消息值间使用“Tab键”进行分隔
 *
 * @author gongyu
 */
public class Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = KafkaConfig.getProducerProperties();
        String topic = "flink-kafka";
        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>() {

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        String key = new String(consumerRecord.key(), "utf8");
                        String value = new String(consumerRecord.value(), "utf8");
                        return key + " : " + value;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return getForClass(String.class);
                    }
                },
                properties));

        streamSource.print();
        env.execute();
    }
}

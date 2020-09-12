package com.gongyu.flink.stream.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author gongyu
 */
public class KafkaConfig {
    private static final String DEFAULT_BROKERS = "node01:9092,node02:9092,node03:9092";
    public static Properties properties = null;

    public static Properties getProducerProperties() {
        return getProducerProperties(DEFAULT_BROKERS);
    }

    public static Properties getProducerProperties(String brokers) {
        synchronized (KafkaConfig.class) {
            if (null == properties) {
                synchronized (KafkaConfig.class) {
                    properties = new Properties();
                    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
                    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer001");
                }
            }
            return properties;
        }
    }
}

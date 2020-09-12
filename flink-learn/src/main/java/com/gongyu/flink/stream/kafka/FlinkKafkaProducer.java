package com.gongyu.flink.stream.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.RandomAccessFile;
import java.util.Properties;

/**
 * @author gongyu
 */
public class FlinkKafkaProducer {
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
        RandomAccessFile file = new RandomAccessFile("./data/data_carFlow_all_column_test.txt", "r");
        String line;
        StringBuilder sb;
        while ((line = new String(file.readLine().getBytes("ISO-8859-1"), "utf8")) != null) {
            System.out.println(line);
            String[] splits = line.split(",");
            String monitorId = splits[0].replace("'", "");
            String carId = splits[2].replace("'", "");
            String timestamp = splits[4].replace("'", "");
            String speed = splits[6];
            sb = new StringBuilder();
            sb.append(monitorId + "\t").append(carId + "\t").append(timestamp + "\t").append(speed);
            producer.send(new ProducerRecord<String, String>("flink-kafka",1 + "", sb.toString()));
            Thread.sleep(300);
        }
    }
}

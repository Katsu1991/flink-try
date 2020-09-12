package com.gongyu.flink.stream.sink;

import com.gongyu.flink.stream.kafka.KafkaConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * 消费kafka中数据，统计各个卡口的流量，并且存入到MySQL中
 *
 * @author gongyu
 */
public class MysqlSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = KafkaConfig.getProducerProperties();

        String topic = "flink-kafka";

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        //此处只需要value数据
                        return new String(consumerRecord.value(), "utf8");
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return getForClass(String.class);
                    }
                },
                props);

        //数据流：310999005201	苏EJ789X	2014-08-20 14:09:43	255
        DataStreamSource<String> stream = env.addSource(flinkKafkaConsumer
        );
        stream
                .map(t -> new Tuple2(t.split("\\s")[0], 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1)
                .addSink(new CustomMysqlSink());

        env.execute();
    }

    private static class CustomMysqlSink extends RichSinkFunction<Tuple2> {
        Connection conn;
        PreparedStatement insertPst;
        PreparedStatement updatePst;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useSSL=false", "root", "root");
            insertPst = conn.prepareStatement("insert into car_flow(monitorId, count) values(?, ?)");
            updatePst = conn.prepareStatement("update car_flow set count = ? where monitorId = ?");
        }

        @Override
        public void invoke(Tuple2 value, Context context) throws Exception {
            updatePst.setInt(1, (int) value.f1);
            updatePst.setString(2, value.f0.toString());
            updatePst.execute();
            if (updatePst.getUpdateCount() == 0) {
                System.out.println("insert...");
                insertPst.setString(1, value.f0.toString());
                insertPst.setInt(2, (int) value.f1);
                insertPst.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertPst.close();
            updatePst.close();
            conn.close();
        }
    }
}

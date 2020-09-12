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

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * @author gongyu
 */
public class SocketSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = KafkaConfig.getProducerProperties();

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
                properties);

        //数据流：310999005201	苏EJ789X	2014-08-20 14:09:43	255
        DataStreamSource<String> stream = env.addSource(flinkKafkaConsumer
        );
        stream
                .map(t -> new Tuple2(t.split("\\s")[0], 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1)
                .addSink(new CustomSocketSink("node04", 8888));

        env.execute();
    }

    private static class CustomSocketSink extends RichSinkFunction<Tuple2> {
        private String host;
        private Integer port;
        private Socket socket;
        private PrintWriter printWriter;

        public CustomSocketSink(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            socket = new Socket(InetAddress.getByName(host), port);
            printWriter = new PrintWriter(socket.getOutputStream());
        }

        @Override
        public void close() throws Exception {
            printWriter.close();
            socket.close();
        }

        @Override
        public void invoke(Tuple2 value, Context context) throws Exception {
            printWriter.print(value.f0 + " - " + value.f1);
            printWriter.flush();
        }
    }

}

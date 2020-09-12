package com.gongyu.flink.stream.sink;

import com.gongyu.flink.stream.kafka.KafkaConfig;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * 如果要写到hdfs，hdfs必须是2。7以上版本
 *
 * @author gongyu
 */
public class FileSink {
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
        SingleOutputStreamOperator<Tuple2> result = stream
                .map(t -> new Tuple2(t.split("\\s")[0], 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1);

        DefaultRollingPolicy rollingPolicy = DefaultRollingPolicy.create()
                //当文件超过2s没有写入新数据，则滚动产生一个小文件
                .withInactivityInterval(2000)
                //文件打开时间超过2s 则滚动产生一个小文件 每隔2s产生一个小文件
                .withRolloverInterval(2000)
                //当文件大小超过256 则滚动产生一个小文件
                .withMaxPartSize(256 * 1024 * 1024)
                .build();

        String filepath = "/Users/gongyu/test/flink/sink";
        StreamingFileSink streamingFileSink = StreamingFileSink.forRowFormat(new Path(filepath), new SimpleStringEncoder<>("utf8"))
                .withBucketCheckInterval(1000)
                .withRollingPolicy(rollingPolicy)
                .build();

        result.addSink(streamingFileSink);

        env.execute();
    }
}

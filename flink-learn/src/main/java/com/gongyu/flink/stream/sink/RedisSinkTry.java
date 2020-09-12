package com.gongyu.flink.stream.sink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * word count 保存到redis
 *
 * @author gongyu
 */
public class RedisSinkTry {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("node04", 8888);
        SingleOutputStreamOperator<Tuple2> result = stream
                .flatMap((String s, Collector<String> collector) -> {
                    String[] values = s.split("\\s");
                    for (String value : values) {
                        if (StringUtils.isNotBlank(value)) {
                            collector.collect(value);
                        }
                    }
                })
                .returns(Types.STRING)
                .map(t -> new Tuple2(t, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1);

        FlinkJedisPoolConfig jedisPoolConfig =
                new FlinkJedisPoolConfig.Builder().setHost("localhost").setPassword("6379").setPassword("password").setDatabase(3).build();
        result.addSink(new org.apache.flink.streaming.connectors.redis.RedisSink<>(jedisPoolConfig, new RedisMapper<Tuple2>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "wc");
            }

            @Override
            public String getKeyFromData(Tuple2 data) {
                return String.valueOf(data.f0);
            }

            @Override
            public String getValueFromData(Tuple2 data) {
                return String.valueOf(data.f1);
            }
        }));

        env.execute();
    }
}

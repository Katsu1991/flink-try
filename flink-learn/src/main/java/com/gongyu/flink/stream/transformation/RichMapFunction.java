package com.gongyu.flink.stream.transformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/**
 * 使用RichMapFunction将单词统计写入redis
 *
 * @author gongyu
 */
public class RichMapFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("node04", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> restStream = stream
                .flatMap((String s, Collector<String> collector) -> {
                    for (String word : s.split("\\s")) {
                        collector.collect(word);
                    }
                })
                .returns(Types.STRING)
                .map(t -> Tuple2.of(t, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1);

        restStream.map(new org.apache.flink.api.common.functions.RichMapFunction<Tuple2<String, Integer>, String>() {

            Jedis jedis = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = new Jedis("localhost", 6379);
                jedis.auth("password");
                jedis.select(2);
                System.out.println("连接redis成功！！！");

                //获取任务名称和子任务名称
                String taskName = getRuntimeContext().getTaskName();
                String taskNameWithSubtasks = getRuntimeContext().getTaskNameWithSubtasks();
                System.out.println("任务名称：" + taskName);
                System.out.println("子务名称：" + taskNameWithSubtasks);
            }

            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                jedis.set(value.f0, value.f1 + "");

                return value.f0;
            }

            @Override
            public void close() {
                jedis.close();
                System.out.println("释放redis资源！！！");
            }
        });

        env.execute();

    }
}

package com.gongyu.flink.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * 多线程发射数据源
 * @author gongyu
 */
public class CustomSourceParallel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.addSource(new ParallelSourceFunction<String>() {
            boolean flag = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    ctx.collect("hello" + random.nextInt(100));
                    Thread.sleep(500);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        }).setParallelism(2);
        streamSource.print().setParallelism(2);
        env.execute();
    }
}

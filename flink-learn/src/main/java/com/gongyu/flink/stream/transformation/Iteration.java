package com.gongyu.flink.stream.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 可以让records反复在算子中执行
 *
 * @author gongyu
 */
public class Iteration {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //控制各算子最大的网络延时
        env.setBufferTimeout(50);
        DataStreamSource<Long> someIntegers = env.generateSequence(9, 10);

        IterativeStream<Long> iteration = someIntegers.iterate();
        SingleOutputStreamOperator<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println(value + "减1");
                return value - 1;
            }
        });

        //筛选出需要回笼的
        SingleOutputStreamOperator<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });
        //进行回笼
        iteration.closeWith(stillGreaterThanZero);

        //筛选出分发到下一个operator的record
        minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value <= 0;
            }
        });

        env.execute();
    }
}

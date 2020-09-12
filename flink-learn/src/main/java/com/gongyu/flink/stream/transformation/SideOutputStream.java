package com.gongyu.flink.stream.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author gongyu
 */
public class SideOutputStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Long> dataStream = env.generateSequence(1, 100).returns(Long.class);

        //主奇(odd)侧偶(even)
        OutputTag<Long> evenOutputTag = new OutputTag<Long>("even"){};
        SingleOutputStreamOperator<Long> processStream = dataStream.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
                if (value % 2 == 0) {
                    ctx.output(evenOutputTag, value);
                } else {
                    out.collect(value);
                }
            }
        });

        DataStream<Long> evenStream = processStream.getSideOutput(evenOutputTag);
        processStream.print("odd");
        evenStream.print("even");

        env.execute();
    }
}

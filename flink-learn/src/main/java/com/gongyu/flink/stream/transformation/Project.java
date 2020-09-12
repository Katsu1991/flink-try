package com.gongyu.flink.stream.transformation;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * 从Tuple中选择属性的子集
 * 仅限event数据类型为Tuple的DataStream
 *
 * （01，zhangsan，25） -> (01, zhangsan)
 *
 * @author gongyu
 */
public class Project {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple4<String, String, String, Integer>> stream = env.fromElements(getTuple4s());
        SingleOutputStreamOperator<Tuple2<String, String>> project = stream.project(0, 1);
        project.print();
        env.execute();
    }

    private static Tuple4<String, String, String, Integer>[] getTuple4s() {
        return new Tuple4[]{
                Tuple4.of("class1", "张三", "语文", 100),
                Tuple4.of("class2", "李四", "语文", 80),
                Tuple4.of("class2", "王五", "语文", 88)
        };
    }
}

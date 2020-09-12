package com.gongyu.flink.stream.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * stream1: id name
 * stream2: id age
 * <p>
 * 输出：id name age  使用join实现(是inner join）
 *
 *
 *
 * @author gongyu
 */
public class Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, String>> singleStream1 = stream1.map(line -> {
            String[] arr = line.split("\\s");
            return new Tuple2<String, String>(arr[0], arr[1]);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        SingleOutputStreamOperator<Tuple2<String, Integer>> singleStream2 = stream2.map(line -> {
            String[] arr = line.split(  "\\s");
            return new Tuple2<String, Integer>(arr[0], Integer.parseInt(arr[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        singleStream1.join(singleStream2)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, Integer>, String>() {
                    @Override
                    public String join(Tuple2<String, String> first, Tuple2<String, Integer> second) throws Exception {
                        return first.f0 + " " + first.f1 + " " + second.f1;
                    }
                })
                .print();


        env.execute();
    }
}

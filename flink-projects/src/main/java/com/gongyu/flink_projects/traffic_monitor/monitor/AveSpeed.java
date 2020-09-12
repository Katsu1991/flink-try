package com.gongyu.flink_projects.traffic_monitor.monitor;

import com.gongyu.flink_projects.traffic_monitor.pojo.AvgSpeedInfo;
import com.gongyu.flink_projects.traffic_monitor.pojo.TrafficInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Properties;

/**
 * 每隔1分钟统计最近5分钟的平均汽车速度
 *
 * @author gongyu
 */
public class AveSpeed {
    private static final String topic = "t_traffic";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9029");
        prop.setProperty("group.id", "avg_001");

        SingleOutputStreamOperator<TrafficInfo> kafkaStream = streamEnv.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop))
                .map(line -> {
                    String[] arr = line.split(",");
                    return new TrafficInfo(Long.parseLong(arr[0]), arr[1], arr[2], arr[3], Double.valueOf(arr[4]), arr[5], arr[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TrafficInfo>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(TrafficInfo element) {
                        return element.getActionTime();
                    }
                });

        kafkaStream.timeWindowAll(Time.seconds(5), Time.seconds(2))
                .process(new ProcessAllWindowFunction<TrafficInfo, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<TrafficInfo> elements, Collector<String> out) throws Exception {
                        Iterator<TrafficInfo> iterator = elements.iterator();
                        int sum = 0;
                        int cnt = 0;
                        while (iterator.hasNext()) {
                            sum += iterator.next().getSpeed();
                            cnt++;
                        }

                        out.collect(sum / cnt + "");
                    }
                }).print();



        kafkaStream.keyBy(t -> t.getMonitorId())
                .timeWindow(Time.minutes(5), Time.minutes(1))
                //设计一个累加器：二元组(车速之后，车辆的数量)
                .aggregate(new AggregateFunction<TrafficInfo, Tuple2<Double, Long>, Tuple2<Double, Long>>() {

                               @Override
                               public Tuple2<Double, Long> createAccumulator() {
                                   return new Tuple2<>(0D, 0L);
                               }

                               @Override
                               public Tuple2<Double, Long> add(TrafficInfo value, Tuple2<Double, Long> acc) {
                                   return new Tuple2(value.getSpeed() + acc.f0, acc.f1 + 1);
                               }

                               @Override
                               public Tuple2<Double, Long> getResult(Tuple2<Double, Long> acc) {
                                   return acc;
                               }

                               @Override
                               public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
                                   return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                               }
                           },
                        new WindowFunction<Tuple2<Double, Long>, AvgSpeedInfo, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<Tuple2<Double, Long>> input, Collector<AvgSpeedInfo> out) throws Exception {
                                Iterator<Tuple2<Double, Long>> iterator = input.iterator();
                                Tuple2<Double, Long> last = null;
                                while (iterator.hasNext()) {
                                    last = iterator.next();
                                }

                                String avgSpeed = String.format("%.2f", last.f0 / last.f1);
                                AvgSpeedInfo info = AvgSpeedInfo.builder()
                                        .start(window.getStart())
                                        .end(window.getEnd())
                                        .monitorId(s)
                                        .avgSpeed(Double.valueOf(avgSpeed))
                                        .carCnt(last.f1.intValue())
                                        .build();

                                out.collect(info);
                            }
                        }).print();

        streamEnv.execute("avg speed");
    }
}
